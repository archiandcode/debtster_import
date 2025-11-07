package processors

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	mg "debtster_import/internal/config/connections/mongo"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/ports"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	debtTeamPrefix  = "debt/"
	defaultUserType = "users"
	defaultAppTeam  = "app"
)

type DistributionDebtsProcessor struct {
	PG *postgres.Postgres
	MG *mg.Mongo

	DebtsTable    string
	UsersTable    string
	TeamsTable    string
	RoleUserTable string

	AppTeamName string

	SystemTeamID int64
}

func (p DistributionDebtsProcessor) Type() string { return "distribution_debts" }

func (p DistributionDebtsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if p.PG == nil || p.PG.Pool == nil {
		return errors.New("postgres not available")
	}
	if p.MG == nil || p.MG.Database == nil {
		return errors.New("mongo not available")
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	debtsTable := p.DebtsTable
	if strings.TrimSpace(debtsTable) == "" {
		debtsTable = "debts"
	}
	usersTable := p.UsersTable
	if strings.TrimSpace(usersTable) == "" {
		usersTable = "users"
	}
	teamsTable := p.TeamsTable
	if strings.TrimSpace(teamsTable) == "" {
		teamsTable = "teams"
	}
	roleUserTable := p.RoleUserTable
	if strings.TrimSpace(roleUserTable) == "" {
		roleUserTable = "role_user"
	}
	appTeamName := p.AppTeamName
	if strings.TrimSpace(appTeamName) == "" {
		appTeamName = defaultAppTeam
	}

	log.Printf("[PROC][redistribute][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	var appTeamID int64
	if err := p.PG.Pool.QueryRow(ctx,
		`SELECT id FROM `+teamsTable+` WHERE name = $1 LIMIT 1`, appTeamName,
	).Scan(&appTeamID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("app team not found: %s", appTeamName)
		}
		return fmt.Errorf("lookup app team failed: %w", err)
	}

	debtIDCache := make(map[string]*string)
	userIDCache := make(map[string]*int64)
	userRoleCache := make(map[int64]*int64)

	type row struct {
		id        string
		payload   map[string]string
		debtID    *string
		userID    *int64
		roleID    *int64
		createdAt *time.Time
	}

	rows := make([]row, 0, len(batch))

	for _, m := range batch {
		debtNumber := strings.TrimSpace(m["debt_number"])
		username := strings.TrimSpace(m["debt_username"])

		if debtNumber == "" {
			logMongoFail(ctx, p.MG, importRecordID, p.Type(), uuid.NewString(), m, "missing debt_number")
			continue
		}
		if username == "" {
			logMongoFail(ctx, p.MG, importRecordID, p.Type(), uuid.NewString(), m, "missing username")
			continue
		}

		var debtUUID *string
		if v, ok := debtIDCache[debtNumber]; ok {
			debtUUID = v
		} else {
			var uuidText string
			if err := p.PG.Pool.QueryRow(ctx,
				`SELECT id::text FROM `+debtsTable+` WHERE number = $1 LIMIT 1`, debtNumber,
			).Scan(&uuidText); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					debtIDCache[debtNumber] = nil
					logMongoFail(ctx, p.MG, importRecordID, p.Type(), uuid.NewString(), m, "debt not found: "+debtNumber)
					continue
				}
				logMongoFail(ctx, p.MG, importRecordID, p.Type(), uuid.NewString(), m, "debt lookup error: "+err.Error())
				continue
			}
			debtUUID = &uuidText
			debtIDCache[debtNumber] = debtUUID
		}
		if debtUUID == nil {
			continue
		}

		var userID *int64
		if v, ok := userIDCache[username]; ok {
			userID = v
		} else {
			var id int64
			if err := p.PG.Pool.QueryRow(ctx,
				`SELECT id FROM `+usersTable+` WHERE username = $1 LIMIT 1`, username,
			).Scan(&id); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					userIDCache[username] = nil
					logMongoFail(ctx, p.MG, importRecordID, p.Type(), uuid.NewString(), m, "username not found: "+username)
					continue
				}
				logMongoFail(ctx, p.MG, importRecordID, p.Type(), uuid.NewString(), m, "user lookup error: "+err.Error())
				continue
			}
			userID = &id
			userIDCache[username] = userID
		}
		if userID == nil {
			continue
		}

		var roleID *int64
		if cached, ok := userRoleCache[*userID]; ok {
			roleID = cached
		} else {
			var rid int64
			err := p.PG.Pool.QueryRow(ctx,
				`SELECT role_id FROM `+roleUserTable+` WHERE user_id = $1 AND team_id = $2 LIMIT 1`,
				*userID, appTeamID,
			).Scan(&rid)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					userRoleCache[*userID] = nil
					logMongoFail(ctx, p.MG, importRecordID, p.Type(), uuid.NewString(), m, "user has no role in app team")
					continue
				}
				logMongoFail(ctx, p.MG, importRecordID, p.Type(), uuid.NewString(), m, "lookup app role failed: "+err.Error())
				continue
			}
			roleID = &rid
			userRoleCache[*userID] = roleID
		}
		if roleID == nil {
			continue
		}

		now := time.Now()
		rows = append(rows, row{
			id:        uuid.NewString(),
			payload:   m,
			debtID:    debtUUID,
			userID:    userID,
			roleID:    roleID,
			createdAt: &now,
		})
	}

	if len(rows) == 0 {
		log.Printf("[PROC][redistribute][DONE] no valid rows")
		return nil
	}

	fixed := 0
	for i, r := range rows {
		debtTeamName := debtTeamPrefix + *r.debtID

		b := &pgx.Batch{}
		b.Queue(
			`UPDATE `+debtsTable+` d
       SET user_id = $2,
           user_assigned_at = NOW()
     WHERE d.id = $1::uuid
       AND (d.user_id IS DISTINCT FROM $2)`,
			r.debtID, r.userID,
		)
		b.Queue(
			`INSERT INTO `+teamsTable+` (name) VALUES ($1)
			 ON CONFLICT (name) DO NOTHING`,
			debtTeamName,
		)
		b.Queue(
			`SELECT id, name FROM `+teamsTable+` WHERE name = $1 LIMIT 1`,
			debtTeamName,
		)

		br := p.PG.Pool.SendBatch(ctx, b)

		if _, err := br.Exec(); err != nil {
			br.Close()
			logMongo(ctx, p.MG, importRecordID, p.Type(), r.id, r.payload, "failed", "update debts: "+err.Error())
			log.Printf("[PROC][redistribute][WARN] row=%d update debts failed: %v", i, err)
			continue
		}

		if _, err := br.Exec(); err != nil {
			br.Close()
			logMongo(ctx, p.MG, importRecordID, p.Type(), r.id, r.payload, "failed", "ensure team: "+err.Error())
			log.Printf("[PROC][redistribute][WARN] row=%d ensure team failed: %v", i, err)
			continue
		}

		var teamID int64
		var teamName string
		if err := br.QueryRow().Scan(&teamID, &teamName); err != nil {
			br.Close()
			logMongo(ctx, p.MG, importRecordID, p.Type(), r.id, r.payload, "failed", "select team_id: "+err.Error())
			log.Printf("[PROC][redistribute][WARN] row=%d select team_id failed: %v", i, err)
			continue
		}
		br.Close()

		if !strings.HasPrefix(teamName, debtTeamPrefix) {
			logMongo(ctx, p.MG, importRecordID, p.Type(), r.id, r.payload, "failed", "team name not debt/*: "+teamName)
			log.Printf("[PROC][redistribute][SKIP] row=%d team(%d,%s) not debt/*", i, teamID, teamName)
			continue
		}

		if _, err := p.PG.Pool.Exec(ctx,
			`DELETE FROM `+roleUserTable+` ru
			   USING `+teamsTable+` tm, `+debtsTable+` d
			 WHERE ru.team_id = tm.id
			   AND tm.id = $1
			   AND tm.name LIKE 'debt/%'
			   AND d.id = $2::uuid
			   AND ru.team_id <> $4
			   AND (ru.user_id <> d.user_id OR (ru.user_id = d.user_id AND ru.role_id <> $3))`,
			teamID, r.debtID, r.roleID, p.SystemTeamID,
		); err != nil {
			logMongo(ctx, p.MG, importRecordID, p.Type(), r.id, r.payload, "failed", "delete wrong role_user: "+err.Error())
			log.Printf("[PROC][redistribute][WARN] row=%d delete role_user failed: %v", i, err)
			continue
		}

		if _, err := p.PG.Pool.Exec(ctx,
			`INSERT INTO `+roleUserTable+` (user_id, role_id, user_type, team_id)
			   SELECT $1, $2, $4, $3
			   WHERE NOT EXISTS (
			     SELECT 1 FROM `+roleUserTable+` ru
			      WHERE ru.user_id = $1 AND ru.role_id = $2 AND ru.team_id = $3
			   )`,
			r.userID, r.roleID, teamID, defaultUserType,
		); err != nil {
			logMongo(ctx, p.MG, importRecordID, p.Type(), r.id, r.payload, "failed", "insert correct role_user: "+err.Error())
			log.Printf("[PROC][redistribute][WARN] row=%d insert role_user failed: %v", i, err)
			continue
		}

		fixed++
		logMongo(ctx, p.MG, importRecordID, p.Type(), r.id, r.payload, "done", "")
	}

	log.Printf("[PROC][redistribute][DONE] total=%d fixed=%d", len(rows), fixed)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][redistribute][ERR] error change status: %v", err)
	}
	return nil
}
