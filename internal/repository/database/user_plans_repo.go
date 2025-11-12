package database

import (
	"context"
	"time"

	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/models"
)

type UserPlanRepo struct {
	PG *postgres.Postgres
}

func NewUserPlanRepo(pg *postgres.Postgres) *UserPlanRepo {
	return &UserPlanRepo{PG: pg}
}

func (r *UserPlanRepo) UpdateOrCreate(ctx context.Context, up models.UserPlan) error {
	if up.UserID == nil {
		return nil
	}
	eom := endOfMonth(up.EndDate)

	tag, err := r.PG.Pool.Exec(ctx, `
		UPDATE user_plans
		   SET amount   = $1::numeric,
		       quantity = $2::bigint,
		       end_date = $3::date
		 WHERE user_id = $4::bigint
		   AND end_date = $3::date
	`, up.Amount, up.Quantity, eom, *up.UserID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		return nil
	}

	_, err = r.PG.Pool.Exec(ctx, `
		INSERT INTO user_plans (user_id, amount, quantity, end_date, created_at)
		VALUES ($1::bigint, $2::numeric, $3::bigint, $4::date, NOW())
	`, *up.UserID, up.Amount, up.Quantity, eom)
	return err
}

func endOfMonth(in *time.Time) time.Time {
	var t time.Time
	if in == nil {
		t = time.Now()
	} else {
		t = *in
	}
	y, m, _ := t.Date()
	loc := t.Location()
	firstNext := time.Date(y, m+1, 1, 0, 0, 0, 0, loc)
	return firstNext.Add(-24 * time.Hour).Truncate(24 * time.Hour)
}
