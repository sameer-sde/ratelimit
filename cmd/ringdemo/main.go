// ringdemo prints the consistent-hash assignment for a fixed set of keys
// across an increasing number of Redis nodes, so you can *see* the property.
package main

import (
	"fmt"

	"github.com/sameer-sde/ratelimit/internal/hashring"
)

func main() {
	keys := []string{
		"user_1", "user_42", "user_100", "user_999",
		"payment:txn_abc", "payment:txn_xyz",
		"api:get-user", "api:create-order",
		"session:sess_aaa", "session:sess_bbb",
	}

	for n := 2; n <= 5; n++ {
		r := hashring.New(150)
		for i := 0; i < n; i++ {
			r.Add(fmt.Sprintf("redis-%d", i))
		}
		fmt.Printf("\n--- %d Redis nodes ---\n", n)
		for _, k := range keys {
			fmt.Printf("  %-25s → %s\n", k, r.Get(k))
		}
	}
}

