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

func TestJudicialRecordOutStructure(t *testing.T) {
	out := JudicialRecordOut{}
	_ = out.JudgeDetail
	_ = out.Evaluations
	_ = out.Metrics
	_ = out.DisciplinaryRecords
}

func TestJudicialEvaluationOutStructure(t *testing.T) {
	out := JudicialEvaluationOut{}
	_ = out.Source
	_ = out.Rating
	_ = out.RatingDate
	_ = out.SourceURL
}

func TestJudicialMetricOutStructure(t *testing.T) {
	out := JudicialMetricOut{}
	_ = out.MetricType
	_ = out.Value
	_ = out.ContextLabel
	_ = out.ComparisonBaseline
	_ = out.TimePeriod
}

func TestJudicialDisciplinaryRecordOutStructure(t *testing.T) {
	out := JudicialDisciplinaryRecordOut{}
	_ = out.RecordType
	_ = out.RecordDate
	_ = out.Description
	_ = out.SourceURL
}
