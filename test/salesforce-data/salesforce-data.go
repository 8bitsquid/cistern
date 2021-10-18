package salesforcedata_test

import (
	"errors"

	"github.com/spf13/afero"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/test"
	rt "gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/test/salesforce-data/rest"
)


const (
	REST test.APIType = iota
	BULKV2
)

var (
	salesforceAPIData test.APITypeMap
	fs afero.Afero
)

func init(){
	fs = afero.Afero{
		Fs: afero.NewBasePathFs(afero.NewOsFs(), "test/salesforce-data"),
	}

	salesforceAPIData = test.APITypeMap{
		REST: rt.Data,
	}
}


func GetSalesforceData(apiType test.APIType, dataType test.DataType) ([]byte, error) {
	if f, ok := salesforceAPIData[apiType][dataType]; ok {
		return fs.ReadFile(f)
	} else {
		panic(errors.New("invalid test api or data api"))
	}
	
}