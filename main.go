package main

import "gitlab.beget.ru/golang/beget_mysqldump/dumper"

func main() {
    dumper := dumper.MakeDumper(&dumper.DumpSettings{
        DatabaseName: "test001",
    })

    dumper.Dump()
}
