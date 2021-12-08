package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime/trace"
	"sync"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	n := flag.Int("n", 4, "parallelism")
	minChunkSize := flag.Int("c", 0, "min chunk size")
	image := flag.String("i", "", "image")
	tag := flag.String("t", "latest", "tag")
	noCache := flag.Bool("nc", false, "no cache")
	registryBaseURL := flag.String("r", "https://public.ecr.aws", "registry base url")
	authBaseURL := flag.String("a", "", "auth base url (default: registry base url)")
	verbose := flag.Bool("v", false, "verbose")
	flag.Parse()

	if *authBaseURL == "" {
		*authBaseURL = *registryBaseURL
	}

	if t := os.Getenv("TRACE"); t != "" {
		f, err := os.Create(t)
		if err != nil {
			return fmt.Errorf("create trace: %w", err)
		}
		defer f.Close()
		if err := trace.Start(f); err != nil {
			return fmt.Errorf("start trace: %w", err)
		}
		defer trace.Stop()
	}

	registryBase, err := url.ParseRequestURI(*registryBaseURL)
	if err != nil {
		return fmt.Errorf("parse registry base url: %w", err)
	}
	authBase, err := url.ParseRequestURI(*authBaseURL)
	if err != nil {
		return fmt.Errorf("parse auth base url: %w", err)
	}

	c := Client{
		c:            http.DefaultClient,
		registryBase: registryBase,
		authBase:     authBase,
		noCache:      *noCache,
		basic:        os.Getenv("TOKEN"),
		verbose:      *verbose,
	}

	ctx, task := trace.NewTask(ctx, "pull")
	defer task.End()

	manifest, err := c.fetchManifest(ctx, *image, *tag)
	if err != nil {
		return fmt.Errorf("fetch manifest: %w", err)
	}

	var chunks []Chunk
	if *minChunkSize != 0 {
		chunks = byChunk(manifest, *n, *minChunkSize*1024*1024)
	} else {
		chunks = byLayer(manifest)
	}

	sem := make(chan struct{}, *n)
	var wg sync.WaitGroup
	for _, chunk := range chunks {
		sem <- struct{}{}
		wg.Add(1)
		go func(chunk Chunk) {
			defer func() {
				wg.Done()
				<-sem
			}()

			log.Println("[INFO] start fetch", *image, chunk.Digest, chunk.Range)
			resp, err := c.fetchBlob(ctx, *image, chunk.Digest, chunk.Range)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
				return
			}
			defer resp.Body.Close()
			defer trace.StartRegion(ctx, "download blob").End()
			pop := resp.Header.Get("X-Amz-Cf-Pop")
			cache := resp.Header.Get("X-Cache")

			buf := make([]byte, 16*1024*1024)
			io.CopyBuffer(io.Discard, resp.Body, buf)
			log.Println("[INFO]", resp.StatusCode, resp.ContentLength, "end fetch", *image, chunk.Digest, chunk.Range, cache, pop)
		}(chunk)
	}
	wg.Wait()
	return nil
}

type Client struct {
	c            *http.Client
	registryBase *url.URL
	authBase     *url.URL
	noCache      bool

	basic   string
	token   string
	verbose bool
}

func (c *Client) logf(format string, args ...interface{}) {
	if c.verbose {
		log.Printf(format, args...)
	}
}

func (c *Client) fetchPullToken(ctx context.Context, image string) (string, error) {
	if c.basic != "" {
		return "Basic " + c.basic, nil
	}
	if !c.noCache && c.token != "" {
		return "Bearer " + c.token, nil
	}

	defer trace.StartRegion(ctx, "fetch pull token").End()
	params := url.Values{}
	params.Set("service", c.registryBase.Host)
	params.Set("scope", "repository:"+image+":pull")

	u := *c.authBase
	u.Path = path.Join(u.Path, "token")
	u.RawQuery = params.Encode()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	resp, err := c.c.Do(req)
	if err != nil {
		return "", fmt.Errorf("request token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		if c.verbose {
			b, _ := io.ReadAll(resp.Body)
			c.logf("[DEBUG] %d %s", resp.StatusCode, string(b))
		}
		return "", fmt.Errorf("request token: unexpected status code: %d", resp.StatusCode)
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}
	c.token = body.Token
	return "Bearer " + c.token, nil
}

func (c *Client) fetchBlob(ctx context.Context, image, digest, r string) (*http.Response, error) {
	u := *c.registryBase
	u.Path = path.Join(u.Path, "v2", image, "blobs", digest)

	token, err := c.fetchPullToken(ctx, image)
	if err != nil {
		return nil, fmt.Errorf("fetch pull token: %w", err)
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	req.Header.Set("Authorization", token)
	if r != "" {
		req.Header.Set("Range", "bytes="+r)
	}
	client := http.Client{
		Transport: &http.Transport{},
	}
	return client.Do(req)
}

func (c *Client) fetchManifest(ctx context.Context, image, ref string) (*Manifest, error) {
	defer trace.StartRegion(ctx, "fetch manifest").End()

	var r io.Reader
	manifestList, err := c.fetchManifestList(ctx, image, ref)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest list: %w", err)
	}
	if len(manifestList.Manifests) > 0 {
		manifestRef := manifestList.find("amd64")
		if manifestRef == nil {
			return nil, fmt.Errorf("manifest not found(arch=amd64)")
		}
		resp, err := c.requestManifest(ctx, image, manifestRef.Digest, manifestRef.MediaType)
		if err != nil {
			return nil, fmt.Errorf("fetch manifest blob: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			if c.verbose {
				b, _ := io.ReadAll(resp.Body)
				c.logf("[DEBUG] %d %s", resp.StatusCode, string(b))
			}
			return nil, fmt.Errorf("fetch manifest blob: unexpected status code: %d", resp.StatusCode)
		}
		r = resp.Body
	} else {
		resp, err := c.requestManifest(ctx, image, ref, "application/vnd.docker.distribution.manifest.v2+json")
		if err != nil {
			return nil, fmt.Errorf("fetch manifest: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			if c.verbose {
				b, _ := io.ReadAll(resp.Body)
				c.logf("[DEBUG] %d %s", resp.StatusCode, string(b))
			}
			return nil, fmt.Errorf("fetch manifest: unexpected status code: %d", resp.StatusCode)
		}
		r = resp.Body
	}
	var manifest Manifest
	if err := json.NewDecoder(r).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	c.logf("[DEBUG] %+v", manifest)
	return &manifest, nil
}

func (c *Client) fetchManifestList(ctx context.Context, image, ref string) (*ManifestList, error) {
	defer trace.StartRegion(ctx, "fetch manifest list").End()
	resp, err := c.requestManifest(ctx, image, ref, "application/vnd.docker.distribution.manifest.list.v2+json")
	if err != nil {
		return nil, fmt.Errorf("request manifest: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("request manifest: unexpected status code: %d", resp.StatusCode)
	}

	var manifestList ManifestList
	if err := json.NewDecoder(resp.Body).Decode(&manifestList); err != nil {
		return nil, fmt.Errorf("parse manifest list response: %w", err)
	}
	c.logf("[DEBUG] %+v", manifestList)
	return &manifestList, nil
}

func (c *Client) requestManifest(ctx context.Context, image, ref, mediaType string) (*http.Response, error) {
	defer trace.StartRegion(ctx, "request manifest").End()
	u := *c.registryBase
	u.Path = path.Join(u.Path, "v2", image, "manifests", ref)

	token, err := c.fetchPullToken(ctx, image)
	if err != nil {
		return nil, fmt.Errorf("fetch pull token: %w", err)
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	req.Header.Set("Authorization", token)
	req.Header.Add("Accept", mediaType)

	c.logf("[DEBUG] GET %s", u.String())
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type ManifestList struct {
	SchemaVersion int           `json:"schemaVersion"`
	MediaType     string        `json:"mediaType"`
	Manifests     []ManifestRef `json:"manifests"`
}

func (m *ManifestList) find(arch string) *ManifestRef {
	for _, manifest := range m.Manifests {
		if manifest.Platform.Architecture == arch {
			return &manifest
		}
	}
	return nil
}

type ManifestRef struct {
	Ref
	Platform Platform `json:"platform"`
}

type Platform struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	Variant      string `json:"variant"`
}

type Manifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	MediaType     string `json:"mediaType"`
	Config        Ref    `json:"config"`
	Layers        []Ref  `json:"layers"`
}

type Ref struct {
	MediaType string `json:"mediaType"`
	Size      int    `json:"size"`
	Digest    string `json:"digest"`
}

func writeFile(name string, r io.Reader) error {
	f, err := os.Create(name)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer f.Close()

	_, err = io.Copy(f, r)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

type Chunk struct {
	Digest string
	Range  string
}

func byLayer(m *Manifest) []Chunk {
	var chunks []Chunk
	for _, layer := range m.Layers {
		chunks = append(chunks, Chunk{Digest: layer.Digest})
	}
	return chunks
}

func byChunk(m *Manifest, n, minSize int) []Chunk {
	total := 0
	for _, layer := range m.Layers {
		total += layer.Size
	}
	chunkSize := total / n
	if chunkSize < minSize {
		chunkSize = minSize
	}
	var chunks []Chunk
	for _, layer := range m.Layers {
		offset := 0
		size := layer.Size
		for size > chunkSize {
			chunks = append(chunks, Chunk{
				Digest: layer.Digest,
				Range:  fmt.Sprintf("%d-%d", offset, offset+chunkSize-1),
			})
			offset += chunkSize
			size -= chunkSize
		}
		chunks = append(chunks, Chunk{
			Digest: layer.Digest,
			Range:  fmt.Sprintf("%d-", offset),
		})
	}
	return chunks
}
