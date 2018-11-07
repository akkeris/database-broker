package broker

import (
	"flag"
)

type Options struct {
	DatabaseUrl string
	NamePrefix  string
}

func AddFlags(o *Options) {
	flag.StringVar(&o.NamePrefix, "name-prefix", "", "The prefix to use on database names (generally short a few characters), you can also set NAME_PREFIX environment var.")
	flag.StringVar(&o.DatabaseUrl, "database-url", "", "The database url to use for storage (e.g., postgres://user:pass@host:port/dbname), you can also set DATABASE_URL environment var.")
}
