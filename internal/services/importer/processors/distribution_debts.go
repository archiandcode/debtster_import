package processors

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"debtster_import/internal/ports"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	debtTeamPrefix = "debt/"
	defaultAppTeam = "app"
)

var defaultUserType = importitems.PHPModelMap[importitems.ModelTypeUsers]

type DistributionDebtsProcessor struct {
	*BaseProcessor

	SystemTeamID int64
}

func (p DistributionDebtsProcessor) Type() string { return "distribution_debts" }

func (p *DistributionDebtsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if err := CheckDeps(p); err != nil {
		return err
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	debtsTable := "debts"
	usersTable := "users"
	teamsTable := "teams"
	roleUserTable := "role_user"
	modelType := p.Type()

	log.Printf("[PROC][redistribute][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	// ---------------------------------------------------------------------
	// Получаем app team
	// ---------------------------------------------------------------------
	var appTeamID int64
	if err := p.PG.Pool.QueryRow(ctx,
		`SELECT id FROM `+teamsTable+` WHERE name = $1 LIMIT 1`, defaultAppTeam,
	).Scan(&appTeamID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("app team not found: %s", defaultAppTeam)
		}
		return fmt.Errorf("lookup app team failed: %w", err)
	}

	// ---------------------------------------------------------------------
	// Кэши для ускорения
	// ---------------------------------------------------------------------
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

	// ---------------------------------------------------------------------
	// 1. Предобработка строк
	// ---------------------------------------------------------------------
	for _, m := range batch {
		rowID := uuid.NewString()

		debtNumber := strings.TrimSpace(m["debt_number"])
		username := strings.TrimSpace(m["debt_username"])

		if debtNumber == "" {
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        rowID,
				Payload:        m,
				Errors:         "missing debt_number",
			})
			continue
		}
		if username == "" {
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        rowID,
				Payload:        m,
				Errors:         "missing username",
			})
			continue
		}

		// ---------------------------------------------
		// 1.1 Получаем debt_id
		// ---------------------------------------------
		var debtUUID *string
		if v, ok := debtIDCache[debtNumber]; ok {
			debtUUID = v
		} else {
			var uuidText string
			err := p.PG.Pool.QueryRow(ctx,
				`SELECT id::text FROM `+debtsTable+` WHERE number = $1 LIMIT 1`, debtNumber,
			).Scan(&uuidText)

			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					debtIDCache[debtNumber] = nil
					importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
						ImportRecordID: importRecordID,
						ModelType:      modelType,
						ModelID:        rowID,
						Payload:        m,
						Errors:         "debt not found: " + debtNumber,
					})
					continue
				}
				importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
					ImportRecordID: importRecordID,
					ModelType:      modelType,
					ModelID:        rowID,
					Payload:        m,
					Errors:         "debt lookup error: " + err.Error(),
				})
				continue
			}
			debtUUID = &uuidText
			debtIDCache[debtNumber] = debtUUID
		}
		if debtUUID == nil {
			continue
		}

		// ---------------------------------------------
		// 1.2 Получаем user_id
		// ---------------------------------------------
		var userID *int64
		if v, ok := userIDCache[username]; ok {
			userID = v
		} else {
			var id int64
			err := p.PG.Pool.QueryRow(ctx,
				`SELECT id FROM `+usersTable+` WHERE username = $1 LIMIT 1`, username,
			).Scan(&id)

			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					userIDCache[username] = nil
					importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
						ImportRecordID: importRecordID,
						ModelType:      modelType,
						ModelID:        rowID,
						Payload:        m,
						Errors:         "username not found: " + username,
					})
					continue
				}
				importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
					ImportRecordID: importRecordID,
					ModelType:      modelType,
					ModelID:        rowID,
					Payload:        m,
					Errors:         "user lookup error: " + err.Error(),
				})
				continue
			}
			userID = &id
			userIDCache[username] = userID
		}
		if userID == nil {
			continue
		}

		// ---------------------------------------------
		// 1.3 Получаем роль пользователя
		// ---------------------------------------------
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
					importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
						ImportRecordID: importRecordID,
						ModelType:      modelType,
						ModelID:        rowID,
						Payload:        m,
						Errors:         "user has no role in app team",
					})
					continue
				}
				importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
					ImportRecordID: importRecordID,
					ModelType:      modelType,
					ModelID:        rowID,
					Payload:        m,
					Errors:         "lookup app role failed: " + err.Error(),
				})
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
			id:        rowID,
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

	// ---------------------------------------------------------------------
	// 2. Основная обработка строк
	// ---------------------------------------------------------------------
	fixed := 0

	for _, r := range rows {
		debtTeamName := debtTeamPrefix + *r.debtID

		batch := &pgx.Batch{}
		batch.Queue(
			`UPDATE `+debtsTable+` d
		     SET user_id = $2,
		         user_assigned_at = (NOW() AT TIME ZONE 'Asia/Almaty')
		     WHERE d.id = $1::uuid AND (d.user_id IS DISTINCT FROM $2)`,
			r.debtID, r.userID,
		)
		batch.Queue(
			`INSERT INTO `+teamsTable+` (name) VALUES ($1)
			 ON CONFLICT (name) DO NOTHING`,
			debtTeamName,
		)
		batch.Queue(
			`SELECT id, name FROM `+teamsTable+` WHERE name = $1 LIMIT 1`,
			debtTeamName,
		)

		br := p.PG.Pool.SendBatch(ctx, batch)

		// 1) UPDATE debts
		if _, err := br.Exec(); err != nil {
			br.Close()
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        r.id,
				Payload:        r.payload,
				Errors:         "update debts: " + err.Error(),
			})
			continue
		}

		// 2) INSERT team
		if _, err := br.Exec(); err != nil {
			br.Close()
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        r.id,
				Payload:        r.payload,
				Errors:         "ensure team: " + err.Error(),
			})
			continue
		}

		// 3) SELECT team_id, name
		var teamID int64
		var teamName string
		if err := br.QueryRow().Scan(&teamID, &teamName); err != nil {
			br.Close()
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        r.id,
				Payload:        r.payload,
				Errors:         "select team_id: " + err.Error(),
			})
			continue
		}
		br.Close()

		if !strings.HasPrefix(teamName, debtTeamPrefix) {
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        r.id,
				Payload:        r.payload,
				Errors:         "team name not debt/*: " + teamName,
			})
			continue
		}

		// Удаляем неправильные записи role_user
		_, err := p.PG.Pool.Exec(ctx,
			`DELETE FROM `+roleUserTable+` ru
			   USING `+teamsTable+` tm, `+debtsTable+` d
			 WHERE ru.team_id = tm.id
			   AND tm.id = $1
			   AND tm.name LIKE 'debt/%'
			   AND d.id = $2::uuid
			   AND ru.team_id <> $4
			   AND (ru.user_id <> d.user_id OR (ru.user_id = d.user_id AND ru.role_id <> $3))`,
			teamID, r.debtID, r.roleID, p.SystemTeamID,
		)
		if err != nil {
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        r.id,
				Payload:        r.payload,
				Errors:         "delete wrong role_user: " + err.Error(),
			})
			continue
		}

		// Добавляем корректную запись role_user
		_, err = p.PG.Pool.Exec(ctx,
			`INSERT INTO `+roleUserTable+` (user_id, role_id, user_type, team_id)
			   SELECT $1, $2, $4, $3
			   WHERE NOT EXISTS (
			     SELECT 1 FROM `+roleUserTable+` ru
			      WHERE ru.user_id = $1 AND ru.role_id = $2 AND ru.team_id = $3
			   )`,
			r.userID, r.roleID, teamID, defaultUserType,
		)
		if err != nil {
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        r.id,
				Payload:        r.payload,
				Errors:         "insert correct role_user: " + err.Error(),
			})
			continue
		}

		fixed++

		// Успешная запись
		importitems.LogMongo(ctx, p.MG, importitems.LogParams{
			ImportRecordID: importRecordID,
			ModelType:      modelType,
			ModelID:        r.id,
			Payload:        r.payload,
			Status:         "done",
			Errors:         "",
		})
	}

	log.Printf("[PROC][redistribute][DONE] total=%d fixed=%d", len(rows), fixed)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][redistribute][ERR] error change status: %v", err)
	}
	return nil
}
