package main

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	migrations "ota-platform/db/migrations"
	"ota-platform/internal/bootstrap"
)

type appliedMigration struct {
	Name      string    `gorm:"column:name;primaryKey"`
	AppliedAt time.Time `gorm:"column:applied_at;autoCreateTime"`
}

func (appliedMigration) TableName() string {
	return "schema_migrations"
}

func main() {
	logger := bootstrap.NewLogger()
	defer logger.Sync()

	db := bootstrap.MustGormDB(logger)
	if err := db.AutoMigrate(&appliedMigration{}); err != nil {
		logger.Fatal("failed to prepare migration table", zap.Error(err))
	}
	files, err := migrations.UpFiles()
	if err != nil {
		logger.Fatal("failed to list migrations", zap.Error(err))
	}
	for _, name := range files {
		var count int64
		if err := db.Model(&appliedMigration{}).Where("name = ?", name).Count(&count).Error; err != nil {
			logger.Fatal("failed to check migration state", zap.String("file", name), zap.Error(err))
		}
		if count > 0 {
			logger.Info("skipping already applied migration", zap.String("file", name))
			continue
		}
		alreadyApplied, err := looksAlreadyApplied(db, name)
		if err != nil {
			logger.Fatal("failed to inspect existing schema", zap.String("file", name), zap.Error(err))
		}
		if alreadyApplied {
			logger.Warn("migration appears already applied; backfilling migration record", zap.String("file", name))
			if err := db.Create(&appliedMigration{Name: name}).Error; err != nil {
				logger.Fatal("failed to backfill migration record", zap.String("file", name), zap.Error(err))
			}
			continue
		}

		sqlBytes, err := migrations.Files.ReadFile(name)
		if err != nil {
			logger.Fatal("failed to read migration", zap.String("file", name), zap.Error(err))
		}
		logger.Info("applying migration", zap.String("file", name))
		if err := db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(string(sqlBytes)).Error; err != nil {
				return err
			}
			return tx.Create(&appliedMigration{Name: name}).Error
		}); err != nil {
			logger.Fatal("failed to apply migration", zap.String("file", name), zap.Error(err))
		}
	}
	fmt.Println("migrations applied")
}

func looksAlreadyApplied(db *gorm.DB, migrationName string) (bool, error) {
	requiredTables := migrationSentinelTables(migrationName)
	if len(requiredTables) == 0 {
		return false, nil
	}

	for _, table := range requiredTables {
		var exists bool
		if err := db.Raw("SELECT to_regclass(?) IS NOT NULL", table).Scan(&exists).Error; err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
	}
	return true, nil
}

func migrationSentinelTables(migrationName string) []string {
	switch migrationName {
	case "001_initial.up.sql":
		return []string{
			"profiles",
			"applications",
			"cards",
			"campaigns",
			"campaign_targets",
			"campaign_shards",
		}
	default:
		return nil
	}
}
