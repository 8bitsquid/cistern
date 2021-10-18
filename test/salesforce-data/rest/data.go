package rest_test

import (
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/test"
)

const (
	SOBJECT_QUERYABLE test.DataType = iota
	SOBJECT_NOT_QUERYABLE
	DESCRIBE_GLOBAL
)

var Data test.DataTypeMap = test.DataTypeMap{
	SOBJECT_QUERYABLE: "rest/rest-describe-ACE_Plan_Initiative__c.json",
	SOBJECT_NOT_QUERYABLE: "rest/rest-describe-ACE_Plan_Initiative__ChangeEvent.json",
	DESCRIBE_GLOBAL: "rest/rest-describe-global.json",
}