package essentials

import (
	"testing"
)

func TestJudgeDetailHasEnrichedFields(t *testing.T) {
	jd := JudgeDetail{}
	_ = jd.ElectionType
	_ = jd.AreasOfFocus
	_ = jd.DateSeated
}

func TestJudicialEvaluationModel(t *testing.T) {
	ev := JudicialEvaluation{}
	_ = ev.PoliticianID
	_ = ev.Source
	_ = ev.Rating
	_ = ev.RatingDate
	_ = ev.SourceURL
	_ = ev.Details

	if ev.TableName() != "essentials.judicial_evaluations" {
		t.Errorf("expected table name essentials.judicial_evaluations, got %s", ev.TableName())
	}
}

func TestJudicialMetricModel(t *testing.T) {
	m := JudicialMetric{}
	_ = m.PoliticianID
	_ = m.MetricType
	_ = m.Value
	_ = m.ContextLabel
	_ = m.ComparisonBaseline
	_ = m.TimePeriod
	_ = m.DataSource
	_ = m.SourceURL

	if m.TableName() != "essentials.judicial_metrics" {
		t.Errorf("expected table name essentials.judicial_metrics, got %s", m.TableName())
	}
}

func TestJudicialDisciplinaryRecordModel(t *testing.T) {
	dr := JudicialDisciplinaryRecord{}
	_ = dr.PoliticianID
	_ = dr.RecordType
	_ = dr.RecordDate
	_ = dr.Description
	_ = dr.SourceURL

	if dr.TableName() != "essentials.judicial_disciplinary_records" {
		t.Errorf("expected table name essentials.judicial_disciplinary_records, got %s", dr.TableName())
	}
}
