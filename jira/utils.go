package jira

import (
	"context"
	"errors"
	"fmt"
	jira "github.com/andygrunwald/go-jira"
	jiraonprem "github.com/andygrunwald/go-jira/v2/onpremise"
	"strings"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

func connect(_ context.Context, d *plugin.QueryData) (*jira.Client, error) {
	// Load connection from cache, which preserves throttling protection etc
	cacheKey := "atlassian-jira"
	if cachedData, ok := d.ConnectionManager.Cache.Get(cacheKey); ok {
		return cachedData.(*jira.Client), nil
	}

	// Start with an empty Turbot config
	var baseUrl, username, token, instanceType string

	// Prefer config options given in Steampipe
	jiraConfig := GetConfig(d.Connection)

	if jiraConfig.InstanceType != nil {
		instanceType = *jiraConfig.InstanceType
	}

	if jiraConfig.BaseUrl != nil {
		baseUrl = *jiraConfig.BaseUrl
	}
	if jiraConfig.Username != nil {
		username = *jiraConfig.Username
	}
	if jiraConfig.Token != nil {
		token = *jiraConfig.Token
	}

	if instanceType == "" {
		return nil, errors.New("'instance_type' must be set in the connection configuration. Edit your connection configuration file and then restart Steampipe")
	}

	if baseUrl == "" {
		return nil, errors.New("'base_url' must be set in the connection configuration. Edit your connection configuration file and then restart Steampipe")
	}

	if username == "" && instanceType == "cloud" {
		return nil, errors.New("'username' must be set in the connection configuration. Edit your connection configuration file and then restart Steampipe")
	}

	if token == "" {
		return nil, errors.New("'token' must be set in the connection configuration. Edit your connection configuration file and then restart Steampipe")
	}

	if instanceType == "cloud" {
		tokenProvider := jira.BasicAuthTransport{}
		tokenProvider.Username = username
		tokenProvider.Password = token

		// Create the client
		client, err := jira.NewClient(tokenProvider.Client(), baseUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating Jira client: %s", err.Error())
		}
		// Save to cache
		d.ConnectionManager.Cache.Set(cacheKey, client)

		// Done
		return client, nil
	}

	if instanceType == "data_center" {
		tokenProvider := jiraonprem.BearerAuthTransport{}
		tokenProvider.Token = token
		// Create the client
		client, err := jira.NewClient(tokenProvider.Client(), baseUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating Jira client: %s", err.Error())
		}
		// Save to cache
		d.ConnectionManager.Cache.Set(cacheKey, client)

		// Done
		return client, nil
	}
	return nil, nil
}

// // Constants
const (
	ColumnDescriptionTitle = "Title of the resource."
)

//// TRANSFORM FUNCTION

// convertJiraTime:: converts jira.Time to time.Time
func convertJiraTime(_ context.Context, d *transform.TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	return time.Time(d.Value.(jira.Time)), nil
}

// convertJiraDate:: converts jira.Date to time.Time
func convertJiraDate(_ context.Context, d *transform.TransformData) (interface{}, error) {
	if d.Value == nil {
		return nil, nil
	}
	return time.Time(d.Value.(jira.Date)), nil
}

func buildJQLQueryFromQuals(equalQuals plugin.KeyColumnQualMap, tableColumns []*plugin.Column) string {
	filters := []string{}

	for _, filterQualItem := range tableColumns {
		filterQual := equalQuals[filterQualItem.Name]
		if filterQual == nil {
			continue
		}

		// Check only if filter qual map matches with optional column name
		if filterQual.Name == filterQualItem.Name {
			if filterQual.Quals == nil {
				continue
			}

			for _, qual := range filterQual.Quals {
				if qual.Value != nil {
					value := qual.Value
					switch filterQualItem.Type {
					case proto.ColumnType_STRING:
						switch qual.Operator {
						case "=":
							filters = append(filters, fmt.Sprintf("\"%s\" = \"%s\"", getIssueJQLKey(filterQualItem.Name), value.GetStringValue()))
						case "<>":
							filters = append(filters, fmt.Sprintf("%s != \"%s\"", getIssueJQLKey(filterQualItem.Name), value.GetStringValue()))
						}
					case proto.ColumnType_TIMESTAMP:
						switch qual.Operator {
						case "=", ">=", ">", "<=", "<":
							filters = append(filters, fmt.Sprintf("\"%s\" %s \"%s\"", getIssueJQLKey(filterQualItem.Name), qual.Operator, value.GetTimestampValue().AsTime().Format("2006-01-02 15:04")))
						case "<>":
							filters = append(filters, fmt.Sprintf("\"%s\" != \"%s\"", getIssueJQLKey(filterQualItem.Name), value.GetTimestampValue().AsTime().Format("2006-01-02 15:04")))
						}

					}
				}
			}

		}
	}

	if len(filters) > 0 {
		return strings.Join(filters, " AND ")
	}

	return ""
}

func getIssueJQLKey(columnName string) string {
	return strings.ToLower(strings.Split(columnName, "_")[0])
}
