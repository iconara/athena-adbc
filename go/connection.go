// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package athena

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	glueSDK "github.com/aws/aws-sdk-go-v2/service/glue"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	athenaClient athenaClientAPI
	glueClient   glueClientAPI
	db           *databaseImpl

	// catalog and schema are per-connection copies of the database defaults,
	// so that SetCurrentCatalog/SetCurrentDbSchema on one connection does not
	// affect sibling connections opened from the same database.
	catalog string
	schema  string
}

func (c *connectionImpl) Close() error {
	c.athenaClient = nil
	c.db = nil
	return nil
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper),
		conn:              c,
	}, nil
}

// GetTableSchema uses Athena's GetTableMetadata API to return an Arrow schema.
func (c *connectionImpl) GetTableSchema(ctx context.Context, catalogName *string, schemaName *string, tableName string) (*arrow.Schema, error) {
	if catalogName == nil || *catalogName == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "catalog is required for GetTableSchema",
		}
	}
	if schemaName == nil || *schemaName == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "schema is required for GetTableSchema",
		}
	}

	out, err := c.athenaClient.GetTableMetadata(ctx, &athenaSDK.GetTableMetadataInput{
		CatalogName:  catalogName,
		DatabaseName: schemaName,
		TableName:    &tableName,
	})
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("GetTableMetadata failed: %v", err),
		}
	}

	fields := make([]arrow.Field, 0, len(out.TableMetadata.Columns))
	for _, col := range out.TableMetadata.Columns {
		name := ""
		if col.Name != nil {
			name = *col.Name
		}
		dt := athenaTypeToArrow(col.Type)
		fields = append(fields, arrow.Field{Name: name, Type: dt, Nullable: true})
	}

	return arrow.NewSchema(fields, nil), nil
}

// CurrentNamespacer interface implementation.

func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.catalog, nil
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return c.schema, nil
}

func (c *connectionImpl) SetCurrentCatalog(catalog string) error {
	c.catalog = catalog
	return nil
}

func (c *connectionImpl) SetCurrentDbSchema(schema string) error {
	c.schema = schema
	return nil
}

// TableTypeLister interface implementation.

func (c *connectionImpl) ListTableTypes(_ context.Context) ([]string, error) {
	return []string{"EXTERNAL_TABLE", "MANAGED_TABLE", "VIRTUAL_VIEW"}, nil
}

// DbObjectsEnumerator interface implementation.

func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	if catalogFilter != nil && *catalogFilter == "" {
		return []string{}, nil
	}

	catalogPattern, err := likePatternToRegex(catalogFilter)
	if err != nil {
		return nil, err
	} else if catalogPattern == nil {
		catalogPattern = regexp.MustCompile("^.*$")
	}

	catalogs, err := c.listAthenaCatalogs(ctx, catalogPattern)
	if err != nil {
		return nil, err
	}

	glueCatalogs, err := c.listGlueCatalogs(ctx, catalogPattern)
	if err != nil {
		return nil, err
	}
	catalogs = append(catalogs, glueCatalogs...)

	return catalogs, nil
}

func (c *connectionImpl) listAthenaCatalogs(ctx context.Context, catalogPattern *regexp.Regexp) ([]string, error) {
	listInput := &athenaSDK.ListDataCatalogsInput{}
	paginator := athenaSDK.NewListDataCatalogsPaginator(c.athenaClient, listInput)

	var catalogs []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("ListDataCatalogs failed: %v", err),
			}
		}
		for _, dc := range page.DataCatalogsSummary {
			if dc.CatalogName != nil && catalogPattern.MatchString(*dc.CatalogName) {
				catalogs = append(catalogs, *dc.CatalogName)
			}
		}
	}
	return catalogs, nil
}

func (c *connectionImpl) listGlueCatalogs(ctx context.Context, catalogPattern *regexp.Regexp) ([]string, error) {
	glueInput := &glueSDK.GetCatalogsInput{Recursive: true}
	glueOut, err := c.glueClient.GetCatalogs(ctx, glueInput)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("Glue GetCatalogs failed: %v", err),
		}
	}

	var catalogs []string
	for _, cat := range glueOut.CatalogList {
		if cat.CatalogId == nil {
			continue
		}
		name := *cat.CatalogId
		if _, after, ok := strings.Cut(name, ":"); ok {
			name = after
		}
		if catalogPattern.MatchString(name) {
			catalogs = append(catalogs, name)
		}
	}
	return catalogs, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	if catalog == "" || (schemaFilter != nil && *schemaFilter == "") {
		return []string{}, nil
	}
	schemaPattern, err := likePatternToRegex(schemaFilter)
	if err != nil {
		return nil, err
	} else if schemaPattern == nil {
		schemaPattern = regexp.MustCompile("^.*$")
	}
	input := &athenaSDK.ListDatabasesInput{
		CatalogName: &catalog,
	}
	paginator := athenaSDK.NewListDatabasesPaginator(c.athenaClient, input)

	var schemas []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			var metadataErr *types.MetadataException
			if errors.As(err, &metadataErr) {
				return nil, nil
			}
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("ListDatabases failed: %v", err),
			}
		}
		for _, db := range page.DatabaseList {
			if db.Name != nil && schemaPattern.MatchString(*db.Name) {
				schemas = append(schemas, *db.Name)
			}
		}
	}
	return schemas, nil
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalogName string, schemaName string, tableFilter *string, _ *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	input := &athenaSDK.ListTableMetadataInput{
		CatalogName:  &catalogName,
		DatabaseName: &schemaName,
	}
	if tableFilter != nil && *tableFilter == "" {
		return []driverbase.TableInfo{}, nil
	} else if tableFilter != nil {
		tableFilterExpression, err := likePatternToRegex(tableFilter)
		if err != nil {
			return nil, err
		}
		expressionStr := tableFilterExpression.String()
		input.Expression = &expressionStr
	}

	paginator := athenaSDK.NewListTableMetadataPaginator(c.athenaClient, input)

	var tables []driverbase.TableInfo
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("ListTableMetadata failed: %v", err),
			}
		}
		for _, tbl := range page.TableMetadataList {
			if tbl.Name == nil {
				continue
			}
			tableType := "EXTERNAL_TABLE"
			if tbl.TableType != nil {
				tableType = *tbl.TableType
			}

			ti := driverbase.TableInfo{
				TableName: *tbl.Name,
				TableType: tableType,
			}

			if includeColumns {
				cols := make([]driverbase.ColumnInfo, 0, len(tbl.Columns))
				for i, col := range tbl.Columns {
					colName := ""
					if col.Name != nil {
						colName = *col.Name
					}
					typeName := ""
					if col.Type != nil {
						typeName = *col.Type
					}
					pos := int32(i + 1)
					cols = append(cols, driverbase.ColumnInfo{
						ColumnName:      colName,
						OrdinalPosition: &pos,
						XdbcTypeName:    &typeName,
					})
				}
				ti.TableColumns = cols
			}

			tables = append(tables, ti)
		}
	}
	return tables, nil
}

func likePatternToRegex(likePattern *string) (*regexp.Regexp, error) {
	if likePattern == nil {
		return regexp.MustCompile("^.*$"), nil
	}
	pat := *likePattern
	var out strings.Builder
	out.Grow(len(pat) * 2)
	out.WriteString("(?i)^")

	isEscape := false
	for i := 0; i < len(pat); i++ {
		ch := pat[i]
		if ch == '\\' && !isEscape {
			isEscape = true
		} else if (ch == '%' || ch == '_') {
			if (isEscape) {
				out.WriteByte(ch)
				isEscape = false;
			} else {
				out.WriteByte('.')
				if ch == '%' {
					out.WriteByte('*')
					for i+1 < len(pat) && pat[i+1] == ch {
						i++
					}
				}
			}
		} else if (isEscape || strings.ContainsRune("?+.[]{}()^$|*\\<>=-!", rune(ch))) {
			out.WriteByte('\\')
			out.WriteByte(ch)
			isEscape = false;
		} else {
			out.WriteByte(ch)
		}
	}
	if isEscape {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "pattern cannot end with an escape",
		}
	}
	out.WriteByte('$')
	r, err := regexp.Compile(out.String())
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("could not compile pattern to regexp: %v", err),
		}
	}
	return r, nil
}

// athenaTypeToArrow converts an Athena column type string to an Arrow DataType.
func athenaTypeToArrow(t *string) arrow.DataType {
	if t == nil {
		return arrow.BinaryTypes.String
	}
	return athenaTypeStringToArrow(*t)
}
