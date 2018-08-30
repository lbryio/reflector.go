package updater

import (
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/lbryio/reflector.go/meta"

	"github.com/inconshreveable/go-update"
	"github.com/sirupsen/logrus"
)

// TODO: this needs to be shut down cleanly

func Run(url, updateCmd string) {
	for {
		<-time.After(1 * time.Minute)

		r, err := http.Head(url)
		if err != nil {
			logrus.Errorln(err)
			continue
		} else if r.StatusCode >= 300 {
			logrus.Errorf("HEAD request returned status code %d", r.StatusCode)
			continue
		}

		lm, err := time.Parse(time.RFC1123, r.Header.Get("Last-Modified"))
		if err != nil {
			logrus.Errorln(err)
			continue
		}

		if !lm.After(meta.BuildTime.Add(5 * time.Minute)) { // buffer for compile and upload time
			continue
		}

		logrus.Println("updating binary...")

		err = updateBin(url)
		if err != nil {
			logrus.Errorln(err)
			// todo: may need to recover here, if stuck without binary
			continue
		}

		parts := strings.Fields(updateCmd)
		err = exec.Command(parts[0], parts[1:]...).Start()
		if err != nil {
			logrus.Errorln(err)
			continue
		}

		return // stop checking for updates in case restart takes a while
	}
}

func updateBin(url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			logrus.Errorln(err)
		}
	}()

	return update.Apply(resp.Body, update.Options{})
}
