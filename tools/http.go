package tools

import (
	"bytes"
	"io"
	"net/http"

	"go.uber.org/zap"
)

func HTTPGetResponseBody(resp *http.Response) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := io.Copy(buf, resp.Body)
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		zap.S().Errorw("HTTP Response Error", "Status", resp.Status, "Message", buf.String())
		// err = errors.New("http response error")
	}

	return buf.Bytes(), err
}
