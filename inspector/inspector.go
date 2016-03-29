package inspector

type Inspector interface {
    Tables(dbName string) ([]string, error)
}