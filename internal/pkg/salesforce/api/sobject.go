package api

type SObject struct {
	Actionoverrides       []SObjectActionoverrides `json:"actionoverrides,omitempty"`
	Activateable          bool                     `json:"activateable,omitempty"`
	Associateentitytype   string                   `json:"associateentitytype,omitempty"`
	Associateparententity string                   `json:"associateparententity,omitempty"`
	Childrelationships    []Childrelationships     `json:"childrelationships"`
	Compactlayoutable     bool                     `json:"compactlayoutable,omitempty"`
	Createable            bool                     `json:"createable,omitempty"`
	Custom                bool                     `json:"custom,omitempty"`
	Customsetting         bool                     `json:"customsetting,omitempty"`
	Deepcloneable         bool                     `json:"deepcloneable,omitempty"`
	Defaultimplementation string                   `json:"defaultimplementation,omitempty"`
	Deletable             bool                     `json:"deletable,omitempty"`
	Deprecatedandhidden   bool                     `json:"deprecatedandhidden,omitempty"`
	Extendedby            string                   `json:"extendedby,omitempty"`
	Extendsinterfaces     string                   `json:"extendsinterfaces,omitempty"`
	Feedenabled           bool                     `json:"feedenabled,omitempty"`
	Fields                []SObjectFields          `json:"fields,omitempty"`
	Hassubtypes           bool                     `json:"hassubtypes,omitempty"`
	Implementedby         string                   `json:"implementedby,omitempty"`
	Implementsinterfaces  string                   `json:"implementsinterfaces,omitempty"`
	Isinterface           bool                     `json:"isinterface,omitempty"`
	Issubtype             bool                     `json:"issubtype,omitempty"`
	Keyprefix             string                   `json:"keyprefix,omitempty"`
	Label                 string                   `json:"label,omitempty"`
	Labelplural           string                   `json:"labelplural,omitempty"`
	Layoutable            bool                     `json:"layoutable,omitempty"`
	Listviewable          string                   `json:"listviewable,omitempty"`
	Lookuplayoutable      string                   `json:"lookuplayoutable,omitempty"`
	Mergeable             bool                     `json:"mergeable,omitempty"`
	Mruenabled            bool                     `json:"mruenabled,omitempty"`
	Name                  string                   `json:"name,omitempty"`
	Namedlayoutinfos      []string                 `json:"namedlayoutinfos,omitempty"`
	Networkscopefieldname string                   `json:"networkscopefieldname,omitempty"`
	Queryable             bool                     `json:"queryable,omitempty"`
	Recordtypeinfos       []Recordtypeinfos        `json:"recordtypeinfos"`
	Replicateable         bool                     `json:"replicateable,omitempty"`
	Retrieveable          bool                     `json:"retrieveable,omitempty"`
	Searchlayoutable      bool                     `json:"searchlayoutable,omitempty"`
	Searchable            bool                     `json:"searchable,omitempty"`
	Sobjectdescribeoption string                   `json:"sobjectdescribeoption,omitempty"`
	SupportedScopes       []SupportedScopes        `json:"supported_scopes,omitempty"`
	Triggerable           bool                     `json:"triggerable,omitempty"`
	Undeletable           bool                     `json:"undeletable,omitempty"`
	Updateable            bool                     `json:"updateable,omitempty"`
	Urls                  SObjectURLs              `json:"urls,omitempty"`
}

type SObjectURLs struct {
	CompactLayouts string `json:"compact_layouts,omitempty"`
	Rowtemplate    string `json:"rowtemplate,omitempty"`
	Eventschema    string `json:"eventschema,omitempty"`
	Describe       string `json:"describe,omitempty"`
	Sobject        string `json:"sobject,omitempty"`
}

type SObjectPicklist struct {
	Active bool `json:"active,omitempty"`
	// Salesforce sometimes sends null, which breaks the very concept of a boolean ಠ_ಠ
	Defaultvalue *bool  `json:"defaultvalue,omitempty"`
	Label        string `json:"label,omitempty"`
	Validfor     string `json:"validfor,omitempty"`
	Value        string `json:"value,omitempty"`
}

type SObjectActionoverrides struct {
	FormFactor         string      `json:"form_factor,omitempty"`
	IsAvailableInTouch *bool   `json:"is_available_in_touch"`
	Name               string      `json:"name,omitempty"`
	PageId             string      `json:"page_id,omitempty"`
	Url                string      `json:"url,omitempty"`
}

type SupportedScopes struct {
	Label string `json:"label,omitempty"`
	Name  string `json:"name,omitempty"`
}

type Recordtypeinfos struct {
	Active                   bool              `json:"active,omitempty"`
	Available                bool              `json:"available,omitempty"`
	DefaultRecordTypeMapping bool              `json:"default_record_type_mapping,omitempty"`
	DeveloperName            string            `json:"developer_name,omitempty"`
	Master                   bool              `json:"master,omitempty"`
	Name                     string            `json:"name,omitempty"`
	RecordTypeId             string            `json:"record_type_id,omitempty"`
	Urls                     map[string]string `json:"urls,omitempty"`
}

type SObjectFields struct {
	Aggregatable      bool   `json:"aggregatable,omitempty"`
	Aipredictionfield bool   `json:"aipredictionfield,omitempty"`
	Autonumber        bool   `json:"autonumber,omitempty"`
	Bytelength        int    `json:"bytelength,omitempty"`
	Calculated        bool   `json:"calculated,omitempty"`
	Calculatedformula string `json:"calculatedformula,omitempty"`
	Cascadedelete     bool   `json:"cascadedelete,omitempty"`
	Casesensitive     bool   `json:"casesensitive,omitempty"`
	Compoundfieldname string `json:"compoundfieldname,omitempty"`
	Controllername    string `json:"controllername,omitempty"`
	Createable        bool   `json:"createable,omitempty"`
	Custom            bool   `json:"custom,omitempty"`
	// Salesforce sometimes sends a boolean, string, or null ಠ_ಠ
	Defaultvalue                 interface{}            `json:"defaultvalue,omitempty"`
	Defaultvalueformula          string            `json:"defaultvalueformula,omitempty"`
	Defaultedoncreate            bool              `json:"defaultedoncreate,omitempty"`
	Dependentpicklist            bool              `json:"dependentpicklist,omitempty"`
	Deprecatedandhidden          bool              `json:"deprecatedandhidden,omitempty"`
	Digits                       int               `json:"digits,omitempty"`
	Displaylocationindecimal     bool              `json:"displaylocationindecimal,omitempty"`
	Encrypted                    bool              `json:"encrypted,omitempty"`
	Externalid                   bool              `json:"externalid,omitempty"`
	Extratypeinfo                string            `json:"extratypeinfo,omitempty"`
	Filterable                   bool              `json:"filterable,omitempty"`
	// TODO: lookup actual structure for this object
	Filteredlookupinfo           interface{}            `json:"filteredlookupinfo,omitempty"`
	Formulatreatnullnumberaszero bool              `json:"formulatreatnullnumberaszero,omitempty"`
	Groupable                    bool              `json:"groupable,omitempty"`
	Highscalenumber              bool              `json:"highscalenumber,omitempty"`
	Htmlformatted                bool              `json:"htmlformatted,omitempty"`
	Idlookup                     bool              `json:"idlookup,omitempty"`
	Inlinehelptext               string            `json:"inlinehelptext,omitempty"`
	Label                        string            `json:"label,omitempty"`
	Length                       int               `json:"length,omitempty"`
	Mask                         string            `json:"mask,omitempty"`
	Masktype                     string            `json:"masktype,omitempty"`
	Name                         string            `json:"name,omitempty"`
	Namefield                    bool              `json:"namefield,omitempty"`
	Namepointing                 bool              `json:"namepointing,omitempty"`
	Nillable                     bool              `json:"nillable,omitempty"`
	Permissionable               bool              `json:"permissionable,omitempty"`
	Picklistvalues               []SObjectPicklist `json:"picklistvalues,omitempty"`
	Polymorphicforeignkey        bool              `json:"polymorphicforeignkey,omitempty"`
	Precision                    int               `json:"precision,omitempty"`
	Querybydistance              bool              `json:"querybydistance,omitempty"`
	Referencetargetfield         string            `json:"referencetargetfield,omitempty"`
	Referenceto                  []string          `json:"referenceto,omitempty"`
	Relationshipname             string            `json:"relationshipname,omitempty"`
	Relationshiporder            int               `json:"relationshiporder,omitempty"`
	Restricteddelete             bool              `json:"restricteddelete,omitempty"`
	Restrictedpicklist           bool              `json:"restrictedpicklist,omitempty"`
	Scale                        int               `json:"scale,omitempty"`
	Searchprefilterable          bool              `json:"searchprefilterable,omitempty"`
	Soaptype                     string            `json:"soaptype,omitempty"`
	Sortable                     bool              `json:"sortable,omitempty"`
	Type                         string            `json:"type,omitempty"`
	Unique                       bool              `json:"unique,omitempty"`
	Updateable                   bool              `json:"updateable,omitempty"`
	Writerequiresmasterread      bool              `json:"writerequiresmasterread,omitempty"`
}

type Childrelationships struct {
	Cascadedelete       bool     `json:"cascadedelete,omitempty"`
	Childsobject        string   `json:"childsobject,omitempty"`
	Deprecatedandhidden bool     `json:"deprecatedandhidden,omitempty"`
	Field               string   `json:"field,omitempty"`
	Junctionidlistnames []string `json:"junctionidlistnames,omitempty"`
	Junctionreferenceto []string `json:"junctionreferenceto,omitempty"`
	Relationshipname    string   `json:"relationshipname,omitempty"`
	Restricteddelete    bool     `json:"restricteddelete,omitempty"`
}
