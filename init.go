package main

import (
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "hermes",
		Usage: "Hermes invalidation-based key-value store implementation",
		Commands: []*cli.Command{
			{
				Name:  "start",
				Usage: "Start a Hermes node",
				Action: func(c *cli.Context) error {
					id := c.Int("id")
					conf := c.String("conf")
					debug := c.Bool("debug")

					node := NewHermesNode(id, conf, debug)
					node.Run()
					return nil
				},
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:     "id",
						Usage:    "Node ID",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "conf",
						Usage: "Path to config file",
						Value: "cluster.conf",
					},
					&cli.BoolFlag{
						Name:  "debug",
						Usage: "Enable debug logging",
						Value: false,
					},
				},
			},
			{
				Name:  "client",
				Usage: "Run the benchmark client",
				Action: func(c *cli.Context) error {
					conf := c.String("conf")
					debug := c.Bool("debug")
					workers := c.Int("workers")
					numKeys := c.Int("keys")
					workloadStr := c.String("workload")

					workload := 50
					switch workloadStr {
					case "ycsb-a":
						workload = 50
					case "ycsb-b":
						workload = 5
					case "ycsb-c":
						workload = 0
					}

					client := NewClient(conf, workload, workers, numKeys, debug)
					client.Run()
					return nil
				},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "conf",
						Usage: "Path to config file",
						Value: "cluster.conf",
					},
					&cli.IntFlag{
						Name:  "workers",
						Usage: "Number of concurrent workers",
						Value: 1,
					},
					&cli.StringFlag{
						Name:  "workload",
						Usage: "Workload type (ycsb-a, ycsb-b, ycsb-c)",
						Value: "ycsb-a",
					},
					&cli.IntFlag{
						Name:  "keys",
						Usage: "Number of keys to use in benchmark",
						Value: 6,
					},
					&cli.BoolFlag{
						Name:  "debug",
						Usage: "Enable debug logging",
						Value: false,
					},
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
