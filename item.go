package nosqlite

import "time"

type Item struct {
	Data    any
	Created time.Time `json:"created,omitempty"`
	Updated time.Time `json:"updated,omitempty"`
}
