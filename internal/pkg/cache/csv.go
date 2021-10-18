package cache

func CSV(i Item) 

// CSV Options
type CSVOption func(co *CSVOptions)

type CSVOptions struct {
	header []string
	nameFromCol string
	splitRows bool
}

func SplitCSVRows() CSVOption {
	return func(co *CSVOptions) {
		co.splitRows = true
	}
}

func NameFromColumn(col string) CSVOption {
	return func(co *CSVOptions) {
		co.nameFromCol = col
	}
}