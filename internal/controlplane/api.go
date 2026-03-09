package controlplane

import (
	"context"
	"crypto/subtle"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	redispkg "ota-platform/internal/redis"
)

// API owns the control-plane HTTP surface end-to-end.
type API struct {
	db       *gorm.DB
	rdb      *redispkg.Client
	campaign *CampaignService
	query    QueryStore
	wsHub    *WSHub
	logger   *zap.Logger

	kpiMu          sync.Mutex
	kpiCached      dashboardKPI
	kpiCachedUntil time.Time
}

func newAPI(database *gorm.DB, rdb *redispkg.Client, campaignSvc *CampaignService, queryStore QueryStore, wsHub *WSHub, logger *zap.Logger) *API {
	api := &API{
		db:       database,
		rdb:      rdb,
		campaign: campaignSvc,
		query:    queryStore,
		wsHub:    wsHub,
		logger:   logger,
	}
	api.registerMetrics()
	return api
}

func apiKeyAuth(logger *zap.Logger) gin.HandlerFunc {
	apiKey := os.Getenv("API_KEY")
	return func(c *gin.Context) {
		if apiKey == "" {
			c.Next()
			return
		}
		auth := c.GetHeader("Authorization")
		token := strings.TrimPrefix(auth, "Bearer ")
		if !strings.HasPrefix(auth, "Bearer ") || subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) != 1 {
			logger.Warn("unauthorized API request",
				zap.String("remote_addr", c.ClientIP()),
				zap.String("path", c.Request.URL.Path),
			)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		c.Next()
	}
}

func (a *API) SetupRouter() *gin.Engine {
	r := gin.Default()
	r.Use(otelgin.Middleware("ota-api"))

	r.MaxMultipartMemory = 10 << 20

	allowedOrigins := strings.Split(os.Getenv("ALLOWED_ORIGINS"), ",")
	if len(allowedOrigins) == 0 || allowedOrigins[0] == "" {
		allowedOrigins = []string{"http://localhost:3000"}
	}

	r.Use(cors.New(cors.Config{
		AllowOrigins:     allowedOrigins,
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	v1 := r.Group("/api/v1")
	v1.Use(apiKeyAuth(a.logger))
	{
		v1.GET("/profiles", a.ListProfiles)
		v1.POST("/profiles", a.CreateProfile)
		v1.GET("/profiles/:id", a.GetProfile)
		v1.PUT("/profiles/:id", a.UpdateProfile)
		v1.DELETE("/profiles/:id", a.DeleteProfile)
		v1.POST("/profiles/:id/applications", a.CreateApplication)
		v1.PUT("/profiles/:id/applications/:appId", a.UpdateApplication)
		v1.DELETE("/profiles/:id/applications/:appId", a.DeleteApplication)

		v1.GET("/cards", a.ListCards)
		v1.POST("/cards", a.CreateCard)
		v1.GET("/cards/:id", a.GetCard)
		v1.PUT("/cards/:id", a.UpdateCard)
		v1.DELETE("/cards/:id", a.DeleteCard)
		v1.GET("/cards/:id/counters", a.GetCardCounters)
		v1.POST("/cards/import", a.ImportCards)
		v1.GET("/cards/export", a.ExportCards)

		v1.GET("/card-groups", a.ListCardGroups)
		v1.POST("/card-groups", a.CreateCardGroup)
		v1.GET("/card-groups/:id", a.GetCardGroup)
		v1.PUT("/card-groups/:id", a.UpdateCardGroup)
		v1.DELETE("/card-groups/:id", a.DeleteCardGroup)
		v1.POST("/card-groups/:id/members", a.AddCardGroupMembers)
		v1.DELETE("/card-groups/:id/members", a.RemoveCardGroupMembers)

		v1.GET("/campaigns", a.ListCampaigns)
		v1.POST("/campaigns", a.CreateCampaign)
		v1.GET("/campaigns/:id", a.GetCampaign)
		v1.POST("/campaigns/:id/start", a.StartCampaign)
		v1.POST("/campaigns/:id/pause", a.PauseCampaign)
		v1.POST("/campaigns/:id/resume", a.ResumeCampaign)
		v1.POST("/campaigns/:id/abort", a.AbortCampaign)
		v1.POST("/campaigns/:id/retry-failed", a.RetryFailedCards)

		v1.GET("/caps", a.ListCAPFiles)
		v1.POST("/caps/upload", a.UploadCAPFile)
		v1.GET("/caps/:id", a.GetCAPFile)
		v1.DELETE("/caps/:id", a.DeleteCAPFile)
		v1.GET("/caps/:id/apdu-preview", a.PreviewCAPAPDUs)

		v1.GET("/scripts", a.ListScripts)
		v1.POST("/scripts", a.CreateScript)
		v1.GET("/scripts/:id", a.GetScript)
		v1.PUT("/scripts/:id", a.UpdateScript)
		v1.DELETE("/scripts/:id", a.DeleteScript)

		v1.GET("/dashboard/kpis", a.GetDashboardKPIs)
		v1.GET("/dashboard/activity", a.GetRecentActivity)
		v1.GET("/dashboard/sms-throughput", a.GetSMSThroughput)

		v1.GET("/monitoring/health", a.GetSystemHealth)
		v1.GET("/monitoring/messages", a.ListMessages)
		v1.GET("/monitoring/messages/:id", a.GetMessage)
		v1.GET("/monitoring/errors", a.GetErrorSummary)

		v1.GET("/debug/card/:id", a.GetDebugCard)
		v1.GET("/debug/campaign/:id", a.GetDebugCampaign)
		v1.GET("/debug/message/:id", a.GetDebugMessage)
		v1.GET("/debug/queues", a.GetDebugQueues)
		v1.GET("/debug/stuck", a.GetDebugStuck)

		v1.GET("/settings", a.GetSettings)
		v1.PUT("/settings", a.UpdateSettings)
	}

	dbg := r.Group("/debug/pprof")
	dbg.Use(apiKeyAuth(a.logger))
	{
		dbg.GET("/", gin.WrapF(pprof.Index))
		dbg.GET("/cmdline", gin.WrapF(pprof.Cmdline))
		dbg.GET("/profile", gin.WrapF(pprof.Profile))
		dbg.POST("/symbol", gin.WrapF(pprof.Symbol))
		dbg.GET("/symbol", gin.WrapF(pprof.Symbol))
		dbg.GET("/trace", gin.WrapF(pprof.Trace))
		dbg.GET("/allocs", gin.WrapH(pprof.Handler("allocs")))
		dbg.GET("/block", gin.WrapH(pprof.Handler("block")))
		dbg.GET("/goroutine", gin.WrapH(pprof.Handler("goroutine")))
		dbg.GET("/heap", gin.WrapH(pprof.Handler("heap")))
		dbg.GET("/mutex", gin.WrapH(pprof.Handler("mutex")))
		dbg.GET("/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))
	}

	r.GET("/ws", func(c *gin.Context) {
		apiKey := os.Getenv("API_KEY")
		if apiKey != "" {
			token := c.Query("token")
			if subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) != 1 {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
				return
			}
		}
		a.wsHub.HandleWebSocket(c.Writer, c.Request)
	})

	return r
}

func paginationParams(c *gin.Context) (page int, perPage int, offset int) {
	page = 1
	perPage = 20

	if v := c.Query("page"); v != "" {
		if p := parseInt(v); p > 0 {
			page = p
		}
	}
	if v := c.Query("page_size"); v != "" {
		if p := parseInt(v); p > 0 && p <= 100 {
			perPage = p
		}
	}
	if v := c.Query("per_page"); v != "" {
		if p := parseInt(v); p > 0 && p <= 100 {
			perPage = p
		}
	}
	offset = (page - 1) * perPage
	return
}

func parseInt(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func errorResponse(c *gin.Context, status int, message string) {
	c.JSON(status, gin.H{"error": message})
}

func paginatedResponse(c *gin.Context, status int, data interface{}, total int64, page, pageSize int) {
	totalPages := int(total) / pageSize
	if int(total)%pageSize != 0 {
		totalPages++
	}
	c.JSON(status, gin.H{
		"data":        data,
		"total":       total,
		"page":        page,
		"page_size":   pageSize,
		"total_pages": totalPages,
	})
}

func parseUUID(c *gin.Context, param string) (string, bool) {
	id := c.Param(param)
	if _, err := uuid.Parse(id); err != nil {
		errorResponse(c, http.StatusBadRequest, "invalid UUID: "+param)
		return "", false
	}
	return id, true
}

func escapeLike(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(s)
}

func (a *API) registerMetrics() {
	meter := otel.Meter("ota-api")
	_, err := meter.Int64ObservableGauge(
		"ota.campaigns.active",
		metric.WithDescription("Number of active campaigns"),
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			var count int64
			if err := a.db.WithContext(ctx).
				Model(&db.Campaign{}).
				Where("status = ?", "running").
				Count(&count).Error; err != nil {
				a.logger.Warn("observe active campaigns", zap.Error(err))
				return err
			}
			observer.Observe(count)
			return nil
		}),
	)
	if err != nil {
		a.logger.Warn("register ota-api metrics", zap.Error(err))
	}
}
