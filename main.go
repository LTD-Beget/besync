package main

import (
    log "github.com/Sirupsen/logrus"
    "github.com/LTD-Beget/besync/modes/proxy"
    "os"
    "io/ioutil"
    "encoding/json"
    "flag"
    "fmt"
)

var version string
var commit string

type Config struct {
    Mode                 string `envconfig:"MODE" default:"http"`
    ModeServerListenHost string `envconfig:"SERVER_LISTEN_HOST" default:"localhost"`
    ModeServerListenPort int    `envconfig:"SERVER_LISTEN_PORT" default:"8080"`
    ModeExportConfigFile string `envconfig:"EXPORT_CONFIG_FILE"`
    Debug                bool
}

var config Config

func init() {
    flag.StringVar(&config.Mode, "mode", "http", "Running mode. May be cli|http")

    // http
    flag.StringVar(&config.ModeServerListenHost, "http-host", "localhost", "[http mode] Listen host")
    flag.IntVar(&config.ModeServerListenPort, "http-port", 8080, "[http mode] Listen port")

    // export
    flag.StringVar(&config.ModeExportConfigFile, "cli-config", "", "[export mode] Json config path")

    flag.BoolVar(&config.Debug, "debug", false, "Enable debug mode")

    // show version if needed
    var showVersion bool
    flag.BoolVar(&showVersion, "version", false, "show besync version")

    flag.Parse()

    if showVersion {
        if version == "" {
            version = "dev"
        }

        fmt.Printf("%s %s\n", version, commit)
        os.Exit(0)
    }

    log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
    log.SetOutput(os.Stdout)

    if config.Debug {
        log.SetLevel(log.DebugLevel)
    } else {
        log.SetLevel(log.InfoLevel)
    }
}

func main() {
    log.Infof("Beget MySQL dumper starting...")
    log.Infof("Mode is '%s'", config.Mode)

    if config.Mode == "cli" {
        var file []byte
        var err error

        if config.ModeExportConfigFile == "" {
            if file, err = ioutil.ReadAll(os.Stdin); err != nil {
                log.Panicf("Stdin read error: %v\n", err)
            }

            if len(file) == 0 {
                log.Panicf("Stdin is empty")
            }
        } else {
            if file, err = ioutil.ReadFile(config.ModeExportConfigFile); err != nil {
                log.Panicf("File error: %v\n", err)
            }
        }

        settings := &proxy.Settings{}
        if err := json.Unmarshal(file, settings); err != nil {
            log.Panicf("Invalid json: %v\n", err)
        }

        resultCh := make(chan *proxy.ExportStatus, 3)

        id, err := proxy.Manager.StartDump(settings, resultCh)
        if err != nil {
            log.Panicf("Exporter error: %v", err)
        }

        L:
        for result := range resultCh {
            log.Infof("GOT EXPORT RESULT %+v", result)

            switch result.Status {
            case "starterd":
                log.Infof("Started")
            case "success":
                log.Infof("Success!")
                break L
            case "error":
                log.Infof("ERROR: %+v", result.Error)
                break L
            }
        }

        log.Infof("Started dump %v", id)
    } else if config.Mode == "http" {
        proxy.Serve(config.ModeServerListenHost, config.ModeServerListenPort)
    }
}
