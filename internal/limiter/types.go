package limiter

// Result is the outcome of a rate-limit check, returned by every algorithm.
type Result struct {
	Allowed   bool
	Current   int64
	Remaining int64
	TTL       int64 // seconds until the limit resets or capacity frees
}

