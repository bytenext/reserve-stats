package schema

import (
	"bytes"
	"fmt"
	"html/template"
	"strings"
	"time"

	"github.com/KyberNetwork/reserve-stats/lib/blockchain"
)

var DateFunctionParams = map[string]string{
	"h": "hour",
	"d": "day",
}

const (
	timeConditionTemplate = `date_trunc('{{.DateParam}}',{{.TimeColumn}}) >= '{{.StartTime}}'` +
		` AND '{{.EndTime}}' >= date_trunc('{{.DateParam}}',{{.TimeColumn}})`

	ethWETHExcludingTmpl = `( NOT EXISTS (SELECT NULL FROM token WHERE address = '{{.ETHTokenAddr}}' AND src_address_id != id )` +
		` OR NOT EXISTS (SELECT NULL FROM token WHERE address = '{{.WETHTokenAddr}}' AND dst_address_id != id ))` +
		` AND ( NOT EXISTS (SELECT NULL FROM token WHERE address = '{{.WETHTokenAddr}}' AND src_address_id != id )` +
		` OR NOT EXISTS (SELECT NULL FROM token WHERE address = '{{.ETHTokenAddr}}' AND dst_address_id != id ))`
)

func BuildDateTruncField(dateTruncParam string, timeZone int8) string {
	if timeZone != 0 && dateTruncParam == "day" {
		var intervalParse = fmt.Sprintf("interval '%d hour'", timeZone)
		return "date_trunc('" + dateTruncParam + "', timestamp + " + intervalParse + ") - " + intervalParse
	}
	return `date_trunc('` + dateTruncParam + `', timestamp)`
}

func RoundTime(t time.Time, freq string, timeZone int8) time.Time {
	if freq == "hour" {
		return t.Truncate(time.Hour)
	}

	return t.Add(time.Duration(timeZone) * time.Hour).Truncate(time.Hour * 24).Add(time.Duration(-timeZone) * time.Hour)
}

func BuildTimeCondition(start time.Time, end time.Time, frequency string) (string, error) {
	var (
		dateFunctionParam string
		resultBuffer      bytes.Buffer
		ok                bool
	)
	tpl, err := template.New("time condition template").Parse(timeConditionTemplate)
	if err != nil {
		return "", err
	}

	if dateFunctionParam, ok = DateFunctionParams[strings.ToLower(frequency)]; !ok {
		return "", fmt.Errorf("invalid burn fee frequency %s", frequency)
	}

	err = tpl.Execute(&resultBuffer, struct {
		DateParam  string
		TimeColumn string
		StartTime  string
		EndTime    string
	}{
		DateParam:  dateFunctionParam,
		TimeColumn: "timestamp",
		StartTime:  start.UTC().Format(DefaultDateFormat),
		EndTime:    end.UTC().Format(DefaultDateFormat),
	})
	if err != nil {
		return "", err
	}
	return resultBuffer.String(), nil
}

// BuildEthWethExcludingCondition creates a condition that filter eth-weth trades
func BuildEthWethExcludingCondition() (string, error) {
	var resultBuffer bytes.Buffer

	tpl, err := template.New("exclude eth template").Parse(ethWETHExcludingTmpl)
	if err != nil {
		return "", nil
	}
	err = tpl.Execute(&resultBuffer, struct {
		ETHTokenAddr  string
		WETHTokenAddr string
	}{
		ETHTokenAddr:  blockchain.ETHAddr.Hex(),
		WETHTokenAddr: blockchain.WETHAddr.Hex(),
	})

	if err != nil {
		return "", nil
	}
	return resultBuffer.String(), nil
}
