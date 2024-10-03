package azure

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/imgproxy/imgproxy/v3/config"
	"github.com/imgproxy/imgproxy/v3/httprange"
	defaultTransport "github.com/imgproxy/imgproxy/v3/transport"
	"github.com/imgproxy/imgproxy/v3/transport/common"
	"github.com/imgproxy/imgproxy/v3/transport/notmodified"
)

type transport struct {
	client                 *azblob.Client
	defaultAzureCredential *azidentity.DefaultAzureCredential
	opts                   *azblob.ClientOptions
}

func New() (http.RoundTripper, error) {
	var (
		client                 *azblob.Client
		sharedKeyCredential    *azblob.SharedKeyCredential
		defaultAzureCredential *azidentity.DefaultAzureCredential
		endpointURL            *url.URL
		err                    error
	)

	endpoint := config.ABSEndpoint
	if len(endpoint) == 0 && len(config.ABSName) > 0 {
		endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", config.ABSName)
	}

	if len(endpoint) > 0 {
		endpointURL, err = url.Parse(endpoint)
		if err != nil {
			return nil, err
		}
	}

	trans, err := defaultTransport.New(false)
	if err != nil {
		return nil, err
	}

	opts := azblob.ClientOptions{
		ClientOptions: policy.ClientOptions{
			Transport: &http.Client{Transport: trans},
		},
	}

	if len(config.ABSKey) > 0 {
		sharedKeyCredential, err = azblob.NewSharedKeyCredential(config.ABSName, config.ABSKey)
		if err != nil {
			return nil, err
		}

		if endpointURL != nil {
			client, err = azblob.NewClientWithSharedKeyCredential(endpointURL.String(), sharedKeyCredential, &opts)
		}
	} else {
		defaultAzureCredential, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}

		if endpointURL != nil {
			client, err = azblob.NewClient(endpointURL.String(), defaultAzureCredential, &opts)
		}
	}

	if err != nil {
		return nil, err
	}

	if endpointURL != nil && defaultAzureCredential == nil {
		defaultAzureCredential, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}
	}

	return transport{client, defaultAzureCredential, &opts}, nil
}

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) {
	container, key := common.GetBucketAndKey(req.URL)

	if len(container) == 0 || len(key) == 0 {
		body := strings.NewReader("Invalid ABS URL: container name or object key is empty")
		return &http.Response{
			StatusCode:    http.StatusNotFound,
			Proto:         "HTTP/1.0",
			ProtoMajor:    1,
			ProtoMinor:    0,
			Header:        http.Header{},
			ContentLength: int64(body.Len()),
			Body:          io.NopCloser(body),
			Close:         false,
			Request:       req,
		}, nil
	}

	var err error
	var endpointURL *url.URL
	if strings.Contains(container, ".blob.core.windows.net") {
		endpointURL, err = url.Parse(fmt.Sprintf("https://%s", container))
		if err != nil {
			return httprange.InvalidHTTPRangeResponse(req), err
		}
	}

	var client *azblob.Client
	if endpointURL != nil && (t.client == nil || endpointURL.String() != t.client.URL()) {
		client, err = azblob.NewClient(endpointURL.String(), t.defaultAzureCredential, t.opts)
		if err != nil {
			return httprange.InvalidHTTPRangeResponse(req), err
		}
		parts := strings.SplitN(key, "/", 2)
		if len(parts) == 2 {
			container = parts[0]
			key = parts[1]
		} else {
			body := strings.NewReader("Invalid ABS URL: unable to split container and key")
			return &http.Response{
				StatusCode:    http.StatusNotFound,
				Proto:         "HTTP/1.0",
				ProtoMajor:    1,
				ProtoMinor:    0,
				Header:        http.Header{},
				ContentLength: int64(body.Len()),
				Body:          io.NopCloser(body),
				Close:         false,
				Request:       req,
			}, nil
		}
	} else {
		client = t.client
	}

	if client == nil {
		body := strings.NewReader("Invalid ABS URL: no endpoint provided and no default client")
		return &http.Response{
			StatusCode:    http.StatusNotFound,
			Proto:         "HTTP/1.0",
			ProtoMajor:    1,
			ProtoMinor:    0,
			Header:        http.Header{},
			ContentLength: int64(body.Len()),
			Body:          io.NopCloser(body),
			Close:         false,
			Request:       req,
		}, nil
	}

	statusCode := http.StatusOK

	header := make(http.Header)
	opts := &blob.DownloadStreamOptions{}

	if r := req.Header.Get("Range"); len(r) != 0 {
		start, end, err := httprange.Parse(r)
		if err != nil {
			return httprange.InvalidHTTPRangeResponse(req), err
		}

		if end != 0 {
			length := end - start + 1
			if end <= 0 {
				length = blockblob.CountToEnd
			}

			opts.Range = blob.HTTPRange{
				Offset: start,
				Count:  length,
			}
		}

		statusCode = http.StatusPartialContent
	}

	result, err := client.DownloadStream(req.Context(), container, key, opts)
	if err != nil {
		if azError, ok := err.(*azcore.ResponseError); !ok || azError.StatusCode < 100 || azError.StatusCode == 301 {
			return nil, err
		} else {
			body := strings.NewReader(azError.Error())
			return &http.Response{
				StatusCode:    azError.StatusCode,
				Proto:         "HTTP/1.0",
				ProtoMajor:    1,
				ProtoMinor:    0,
				Header:        header,
				ContentLength: int64(body.Len()),
				Body:          io.NopCloser(body),
				Close:         false,
				Request:       req,
			}, nil
		}
	}

	if config.ETagEnabled && result.ETag != nil {
		etag := string(*result.ETag)
		header.Set("ETag", etag)
	}
	if config.LastModifiedEnabled && result.LastModified != nil {
		lastModified := result.LastModified.Format(http.TimeFormat)
		header.Set("Last-Modified", lastModified)
	}

	if resp := notmodified.Response(req, header); resp != nil {
		if result.Body != nil {
			result.Body.Close()
		}
		return resp, nil
	}

	header.Set("Accept-Ranges", "bytes")

	contentLength := int64(0)
	if result.ContentLength != nil {
		contentLength = *result.ContentLength
		header.Set("Content-Length", strconv.FormatInt(*result.ContentLength, 10))
	}

	if result.ContentType != nil {
		header.Set("Content-Type", *result.ContentType)
	}

	if result.ContentRange != nil {
		header.Set("Content-Range", *result.ContentRange)
	}

	if result.CacheControl != nil {
		header.Set("Cache-Control", *result.CacheControl)
	}

	return &http.Response{
		StatusCode:    statusCode,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Header:        header,
		ContentLength: contentLength,
		Body:          result.Body,
		Close:         true,
		Request:       req,
	}, nil
}
