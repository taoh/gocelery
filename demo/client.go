// +build ignore

package main

import (
	"fmt"

	"github.com/taoh/gocelery"
)

func main() {
	fmt.Println("Client")
	args := []interface{}{7, 2}

	gocelery.PublishTask("", "tasks.add", args)
}
