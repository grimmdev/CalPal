package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	_ "time/tzdata"

	"github.com/apognu/gocal"
	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

//go:embed templates/*
//go:embed static/*
var resources embed.FS

var (
	db       *sql.DB
	tpl      *template.Template
	icsCache = make(map[int][]gocal.Event)
	icsMutex sync.Mutex
)

var (
	port     = os.Getenv("PORT")
	baseURL  = os.Getenv("BASE_URL")
	smtpHost = os.Getenv("SMTP_HOST")
	smtpPort = os.Getenv("SMTP_PORT")
	smtpUser = os.Getenv("SMTP_USER")
	smtpPass = os.Getenv("SMTP_PASS")
	smtpFrom = os.Getenv("SMTP_FROM")

	googleClientID     = os.Getenv("GOOGLE_CLIENT_ID")
	googleClientSecret = os.Getenv("GOOGLE_CLIENT_SECRET")

	microsoftClientID     = os.Getenv("MICROSOFT_CLIENT_ID")
	microsoftClientSecret = os.Getenv("MICROSOFT_CLIENT_SECRET")
	microsoftTenantID     = os.Getenv("MICROSOFT_TENANT_ID")
)

type User struct {
	ID                    int
	Username              string
	Email                 string
	IcsURL                string
	Timezone              string
	WorkDays              string
	WorkStart             string
	WorkEnd               string
	GoogleRefreshToken    string
	MicrosoftRefreshToken string
	BookingTemplate       string
	CancelTemplate        string
}

type EventType struct {
	ID              int
	UserID          int
	Title           string
	Slug            string
	Duration        int
	Description     string
	Location        string
	MeetingType     string
	EmailTemplate   string
	ReminderMinutes int
	IsActive        bool
}

type Booking struct {
	ID               int
	EventID          int
	EventTitle       string
	StartTime        string
	Name             string
	Email            string
	Phone            string
	Status           string
	GoogleEventID    string
	MicrosoftEventID string
}

func main() {
	if port == "" {
		port = "8080"
	}
	if baseURL == "" {
		baseURL = "http://localhost:" + port
	}
	if microsoftTenantID == "" {
		microsoftTenantID = "common"
	}

	var err error
	os.MkdirAll("data", 0755)
	db, err = sql.Open("sqlite", "data/calpal.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	initDB()
	go startReminderDaemon()

	funcMap := template.FuncMap{
		"contains": strings.Contains,
	}
	tpl, err = template.New("").Funcs(funcMap).ParseFS(resources, "templates/*.html")
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /static/", http.FileServer(http.FS(resources)))
	mux.HandleFunc("GET /", handleHome)
	mux.HandleFunc("POST /signup", handleSignup)
	mux.HandleFunc("POST /login", handleLogin)
	mux.HandleFunc("GET /logout", handleLogout)

	mux.HandleFunc("GET /dashboard", requireAuth(handleDashboard))
	mux.HandleFunc("POST /dashboard/settings", requireAuth(handleSettings))
	mux.HandleFunc("POST /dashboard/event", requireAuth(handleCreateEvent))
	mux.HandleFunc("POST /dashboard/event/delete", requireAuth(handleDeleteEvent))
	mux.HandleFunc("POST /dashboard/booking/cancel", requireAuth(handleCancelBooking))

	mux.HandleFunc("GET /auth/google", requireAuth(handleGoogleConnect))
	mux.HandleFunc("GET /auth/google/callback", handleGoogleCallback)
	mux.HandleFunc("POST /auth/google/disconnect", requireAuth(handleGoogleDisconnect))

	mux.HandleFunc("GET /auth/microsoft", requireAuth(handleMicrosoftConnect))
	mux.HandleFunc("GET /auth/microsoft/callback", handleMicrosoftCallback)
	mux.HandleFunc("POST /auth/microsoft/disconnect", requireAuth(handleMicrosoftDisconnect))

	mux.HandleFunc("GET /u/", handlePublicPage)
	mux.HandleFunc("POST /u/", handlePublicBooking)

	fmt.Printf("CalPal running at %s (port %s)\n", baseURL, port)
	if smtpHost != "" {
		fmt.Printf("Email enabled via %s:%s\n", smtpHost, smtpPort)
	}
	if googleClientID != "" {
		fmt.Printf("Google Calendar integration enabled\n")
	}
	if microsoftClientID != "" {
		fmt.Printf("Microsoft Calendar integration enabled\n")
	}
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func initDB() {
	db.Exec(`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT UNIQUE, email TEXT, passhash TEXT, ics_url TEXT, timezone TEXT DEFAULT 'America/New_York');`)
	db.Exec(`CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, user_id INTEGER, expires DATETIME);`)
	db.Exec(`CREATE TABLE IF NOT EXISTS event_types (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, slug TEXT, duration INTEGER);`)
	db.Exec(`CREATE TABLE IF NOT EXISTS bookings (id INTEGER PRIMARY KEY, event_id INTEGER, start_utc DATETIME, name TEXT, email TEXT, phone TEXT);`)

	db.Exec("ALTER TABLE users ADD COLUMN work_days TEXT DEFAULT '1,2,3,4,5'")
	db.Exec("ALTER TABLE users ADD COLUMN work_start TEXT DEFAULT '09:00'")
	db.Exec("ALTER TABLE users ADD COLUMN work_end TEXT DEFAULT '17:00'")
	db.Exec("ALTER TABLE users ADD COLUMN google_refresh_token TEXT DEFAULT ''")
	db.Exec("ALTER TABLE users ADD COLUMN microsoft_refresh_token TEXT DEFAULT ''")
	db.Exec("ALTER TABLE users ADD COLUMN booking_template TEXT DEFAULT ''")
	db.Exec("ALTER TABLE users ADD COLUMN cancel_template TEXT DEFAULT ''")

	db.Exec("UPDATE users SET work_days = '1,2,3,4,5' WHERE work_days IS NULL OR work_days = ''")
	db.Exec("UPDATE users SET work_start = '09:00' WHERE work_start IS NULL OR work_start = ''")
	db.Exec("UPDATE users SET work_end = '17:00' WHERE work_end IS NULL OR work_end = ''")

	db.Exec("ALTER TABLE event_types ADD COLUMN description TEXT DEFAULT ''")
	db.Exec("ALTER TABLE event_types ADD COLUMN location TEXT DEFAULT ''")
	db.Exec("ALTER TABLE event_types ADD COLUMN is_active INTEGER DEFAULT 1")
	db.Exec("ALTER TABLE event_types ADD COLUMN email_template TEXT DEFAULT ''")
	db.Exec("ALTER TABLE event_types ADD COLUMN reminder_minutes INTEGER DEFAULT 0")

	db.Exec("ALTER TABLE bookings ADD COLUMN status TEXT DEFAULT 'active'")
	db.Exec("ALTER TABLE bookings ADD COLUMN invitee_tz TEXT DEFAULT 'America/New_York'")
	db.Exec("ALTER TABLE bookings ADD COLUMN reminder_sent INTEGER DEFAULT 0")
	db.Exec("ALTER TABLE event_types ADD COLUMN meeting_type TEXT DEFAULT 'custom_link'")
	db.Exec("ALTER TABLE bookings ADD COLUMN invitee_phone TEXT DEFAULT ''")
	db.Exec("ALTER TABLE bookings ADD COLUMN google_event_id TEXT DEFAULT ''")
	db.Exec("ALTER TABLE bookings ADD COLUMN microsoft_event_id TEXT DEFAULT ''")
}

func parseLocation(tz string) *time.Location {
	if tz == "" {
		tz = "America/New_York"
	}
	tz = strings.ReplaceAll(tz, "%2f", "/")
	tz = strings.ReplaceAll(tz, "%2F", "/")
	upper := strings.ToUpper(strings.TrimSpace(tz))
	switch upper {
	case "EST", "EDT", "EASTERN":
		tz = "America/New_York"
	case "CST", "CDT", "CENTRAL":
		tz = "America/Chicago"
	case "MST", "MDT", "MOUNTAIN":
		tz = "America/Denver"
	case "PST", "PDT", "PACIFIC":
		tz = "America/Los_Angeles"
	case "UTC", "GMT":
		return time.UTC
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		fmt.Printf("Timezone '%s' not recognized, defaulting to America/New_York\n", tz)
		loc, _ = time.LoadLocation("America/New_York")
	}
	if loc == nil {
		return time.UTC
	}
	return loc
}

func generateCalURLs(title, desc, loc string, start, end time.Time) (string, string) {
	gStart, gEnd := start.UTC().Format("20060102T150405Z"), end.UTC().Format("20060102T150405Z")
	google := fmt.Sprintf("https://calendar.google.com/calendar/render?action=TEMPLATE&text=%s&dates=%s/%s&details=%s&location=%s",
		url.QueryEscape(title), gStart, gEnd, url.QueryEscape(desc), url.QueryEscape(loc))

	oStart, oEnd := start.UTC().Format("2006-01-02T15:04:05Z"), end.UTC().Format("2006-01-02T15:04:05Z")
	outlook := fmt.Sprintf("https://outlook.live.com/calendar/0/deeplink/compose?path=/calendar/action/compose&rru=addevent&subject=%s&startdt=%s&enddt=%s&body=%s&location=%s",
		url.QueryEscape(title), url.QueryEscape(oStart), url.QueryEscape(oEnd), url.QueryEscape(desc), url.QueryEscape(loc))

	return google, outlook
}

func sendEmail(to, subject, htmlBody string) {
	if smtpHost == "" || smtpPort == "" {
		return
	}

	msg := "To: " + to + "\r\n" +
		"From: " + smtpFrom + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: text/html; charset=\"UTF-8\"\r\n" +
		"Content-Transfer-Encoding: quoted-printable\r\n" +
		"\r\n" +
		toQuotedPrintable(htmlBody) + "\r\n"

	smtpSend(to, []byte(msg))
}

func extractEmail(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.Index(s, "<"); i >= 0 {
		if j := strings.Index(s, ">"); j > i {
			return strings.TrimSpace(s[i+1 : j])
		}
	}
	return s
}

func buildICS(uid, summary, description, location string, start, end time.Time, organizerEmail, attendeeEmail string) string {
	const dtFmt = "20060102T150405Z"
	now := time.Now().UTC()

	fold := func(line string) string {
		if len(line) <= 75 {
			return line + "\r\n"
		}
		var b strings.Builder
		b.WriteString(line[:75] + "\r\n")
		line = line[75:]
		for len(line) > 0 {
			if len(line) > 74 {
				b.WriteString(" " + line[:74] + "\r\n")
				line = line[74:]
			} else {
				b.WriteString(" " + line + "\r\n")
				line = ""
			}
		}
		return b.String()
	}

	orgEmail := extractEmail(organizerEmail)
	attEmail := extractEmail(attendeeEmail)

	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\n")
	b.WriteString("VERSION:2.0\r\n")
	b.WriteString("PRODID:-//CalPal//CalPal//EN\r\n")
	b.WriteString("CALSCALE:GREGORIAN\r\n")
	b.WriteString("METHOD:REQUEST\r\n")
	b.WriteString("BEGIN:VEVENT\r\n")
	b.WriteString(fold("UID:" + uid))
	b.WriteString(fold("DTSTAMP:" + now.Format(dtFmt)))
	b.WriteString(fold("DTSTART:" + start.UTC().Format(dtFmt)))
	b.WriteString(fold("DTEND:" + end.UTC().Format(dtFmt)))
	b.WriteString(fold("SUMMARY:" + summary))
	if description != "" {
		desc := strings.ReplaceAll(description, "\n", "\\n")
		desc = strings.ReplaceAll(desc, ",", "\\,")
		b.WriteString(fold("DESCRIPTION:" + desc))
	}
	if location != "" {
		b.WriteString(fold("LOCATION:" + strings.ReplaceAll(location, ",", "\\,")))
	}
	if orgEmail != "" {
		b.WriteString(fold("ORGANIZER:mailto:" + orgEmail))
	}
	if attEmail != "" {
		b.WriteString(fold("ATTENDEE;CUTYPE=INDIVIDUAL;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:" + attEmail))
	}
	b.WriteString("BEGIN:VALARM\r\n")
	b.WriteString("TRIGGER:-PT15M\r\n")
	b.WriteString("ACTION:DISPLAY\r\n")
	b.WriteString("DESCRIPTION:Reminder\r\n")
	b.WriteString("END:VALARM\r\n")
	b.WriteString("END:VEVENT\r\n")
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

func sendEmailWithICS(to, subject, htmlBody, icsContent, icsFilename string) {
	if smtpHost == "" || smtpPort == "" {
		return
	}

	outerBoundary := fmt.Sprintf("CalPalOuter%d", time.Now().UnixNano())
	innerBoundary := fmt.Sprintf("CalPalInner%d", time.Now().UnixNano()+1)

	icsB64 := base64.StdEncoding.EncodeToString([]byte(icsContent))
	var chunked strings.Builder
	for i := 0; i < len(icsB64); i += 76 {
		end := i + 76
		if end > len(icsB64) {
			end = len(icsB64)
		}
		chunked.WriteString(icsB64[i:end] + "\r\n")
	}

	var msg strings.Builder
	msg.WriteString("To: " + to + "\r\n")
	msg.WriteString("From: " + smtpFrom + "\r\n")
	msg.WriteString("Subject: " + subject + "\r\n")
	msg.WriteString("MIME-Version: 1.0\r\n")
	msg.WriteString(fmt.Sprintf("Content-Type: multipart/mixed; boundary=\"%s\"\r\n", outerBoundary))
	msg.WriteString("\r\n")

	msg.WriteString(fmt.Sprintf("--%s\r\n", outerBoundary))
	msg.WriteString(fmt.Sprintf("Content-Type: multipart/alternative; boundary=\"%s\"\r\n", innerBoundary))
	msg.WriteString("\r\n")

	msg.WriteString(fmt.Sprintf("--%s\r\n", innerBoundary))
	msg.WriteString("Content-Type: text/html; charset=\"UTF-8\"\r\n")
	msg.WriteString("Content-Transfer-Encoding: quoted-printable\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(toQuotedPrintable(htmlBody) + "\r\n")

	msg.WriteString(fmt.Sprintf("--%s\r\n", innerBoundary))
	msg.WriteString("Content-Type: text/calendar; charset=\"UTF-8\"; method=REQUEST\r\n")
	msg.WriteString("Content-Transfer-Encoding: 7bit\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(icsContent + "\r\n")
	msg.WriteString(fmt.Sprintf("--%s--\r\n", innerBoundary))

	msg.WriteString(fmt.Sprintf("--%s\r\n", outerBoundary))
	msg.WriteString(fmt.Sprintf("Content-Type: application/ics; name=\"%s\"\r\n", icsFilename))
	msg.WriteString(fmt.Sprintf("Content-Disposition: attachment; filename=\"%s\"\r\n", icsFilename))
	msg.WriteString("Content-Transfer-Encoding: base64\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(chunked.String())
	msg.WriteString(fmt.Sprintf("--%s--\r\n", outerBoundary))

	raw := []byte(msg.String())
	smtpSend(to, raw)
}

func toQuotedPrintable(s string) string {
	var buf strings.Builder
	lineLen := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\r' {
			continue
		}
		if c == '\n' {
			buf.WriteString("\r\n")
			lineLen = 0
			continue
		}
		var encoded string
		if c == '=' {
			encoded = "=3D"
		} else if c > 126 || (c < 32 && c != '\t') {
			encoded = fmt.Sprintf("=%02X", c)
		} else {
			encoded = string(c)
		}
		if lineLen+len(encoded) > 75 {
			buf.WriteString("=\r\n")
			lineLen = 0
		}
		buf.WriteString(encoded)
		lineLen += len(encoded)
	}
	return buf.String()
}

func smtpSend(to string, raw []byte) {
	if smtpPort == "465" {
		tlsCfg := &tls.Config{ServerName: smtpHost}
		conn, err := tls.Dial("tcp", smtpHost+":"+smtpPort, tlsCfg)
		if err != nil {
			fmt.Printf("📧 SMTP TLS dial error: %v\n", err)
			return
		}
		defer conn.Close()
		client, err := smtp.NewClient(conn, smtpHost)
		if err != nil {
			fmt.Printf("📧 SMTP client error: %v\n", err)
			return
		}
		defer client.Close()
		auth := smtp.PlainAuth("", smtpUser, smtpPass, smtpHost)
		if err = client.Auth(auth); err != nil {
			fmt.Printf("📧 SMTP auth error: %v\n", err)
			return
		}
		if err = client.Mail(extractEmail(smtpFrom)); err != nil {
			fmt.Printf("📧 SMTP MAIL FROM error: %v\n", err)
			return
		}
		if err = client.Rcpt(to); err != nil {
			fmt.Printf("📧 SMTP RCPT TO error: %v\n", err)
			return
		}
		w, err := client.Data()
		if err != nil {
			fmt.Printf("📧 SMTP DATA error: %v\n", err)
			return
		}
		w.Write(raw)
		w.Close()
		client.Quit()
	} else {
		auth := smtp.PlainAuth("", smtpUser, smtpPass, smtpHost)
		tlsCfg := &tls.Config{ServerName: smtpHost}
		err := smtp.SendMail(smtpHost+":"+smtpPort, auth, extractEmail(smtpFrom), []string{to}, raw)
		if err != nil {
			conn, dialErr := smtp.Dial(smtpHost + ":" + smtpPort)
			if dialErr != nil {
				fmt.Printf("📧 SMTP dial error: %v\n", err)
				return
			}
			defer conn.Close()
			if err = conn.StartTLS(tlsCfg); err != nil {
				fmt.Printf("📧 SMTP STARTTLS error: %v\n", err)
				return
			}
			if err = conn.Auth(auth); err != nil {
				fmt.Printf("📧 SMTP auth error: %v\n", err)
				return
			}
			if err = conn.Mail(extractEmail(smtpFrom)); err != nil {
				fmt.Printf("📧 SMTP MAIL FROM error: %v\n", err)
				return
			}
			if err = conn.Rcpt(to); err != nil {
				fmt.Printf("📧 SMTP RCPT TO error: %v\n", err)
				return
			}
			w, err := conn.Data()
			if err != nil {
				fmt.Printf("📧 SMTP DATA error: %v\n", err)
				return
			}
			w.Write(raw)
			w.Close()
			conn.Quit()
		}
	}
}

func getDefaultEmailTemplate() string {
	content, err := resources.ReadFile("templates/email_template.html")
	if err == nil {
		return string(content)
	}
	return `<div style="font-family:sans-serif;color:#333;max-width:600px;margin:0 auto;">` +
		`<h2 style="color:#8e44ad;">Booking Confirmed! 🎉</h2>` +
		`<p>Hi <b>[Name]</b>,</p>` +
		`<p>You are booked for <b>[EventName]</b> with <b>[HostName]</b>.</p>` +
		`<div style="background:#f8fafc;border:1px solid #e2e8f0;padding:20px;border-radius:8px;margin:25px 0;">` +
		`<p style="margin:0 0 12px 0;">📅 <b>Time:</b> [Time] <span style="color:#64748b;font-size:0.9em;">(Your timezone)</span></p>` +
		`<p style="margin:0;">📍 <b>Location/Link:</b> [Location]</p></div>` +
		`<div style="margin: 30px 0;"><p style="font-size: 0.9em; color: #64748b; margin-bottom: 12px; font-weight: bold;">Add to your calendar:</p>` +
		`<a href="[GoogleCalURL]" style="display: inline-block; padding: 10px 18px; background: #4285F4; color: white; text-decoration: none; border-radius: 6px; margin-right: 10px; font-size: 0.9em; font-weight: bold;">📅 Google</a>` +
		`<a href="[OutlookCalURL]" style="display: inline-block; padding: 10px 18px; background: #0078D4; color: white; text-decoration: none; border-radius: 6px; font-size: 0.9em; font-weight: bold;">📅 Outlook</a></div>` +
		`<p style="color:#64748b;">If you need to make any changes, please reply directly to this email.</p>` +
		`<p>See you soon!</p></div>`
}

func getDefaultCancelTemplate() string {
	content, err := resources.ReadFile("templates/cancel_template.html")
	if err == nil {
		return string(content)
	}
	return `<div style="font-family:sans-serif;color:#333;max-width:600px;margin:0 auto;">` +
		`<h2 style="color:#ef4444;">Meeting Cancelled 😔</h2>` +
		`<p>Hi <b>[Name]</b>,</p>` +
		`<p>Your booking for <b>[EventName]</b> with <b>[HostName]</b> has been cancelled.</p>` +
		`<div style="background:#fee2e2;border:1px solid #fecaca;padding:20px;border-radius:8px;margin:25px 0;">` +
		`<p style="margin:0 0 12px 0;">📅 <b>Was scheduled for:</b> [Time] <span style="color:#64748b;font-size:0.9em;">(Your timezone)</span></p>` +
		`</div>` +
		`<p style="color:#64748b;">If you'd like to reschedule, please visit the booking page or reply to this email.</p>` +
		`<p>Sorry for the inconvenience.</p></div>`
}

func formatCancelTemplate(tmpl, name, eventName, hostName, timeStr string) string {
	if tmpl == "" {
		tmpl = getDefaultCancelTemplate()
	}
	res := strings.ReplaceAll(tmpl, "[Name]", name)
	res = strings.ReplaceAll(res, "[EventName]", eventName)
	res = strings.ReplaceAll(res, "[HostName]", hostName)
	res = strings.ReplaceAll(res, "[Time]", timeStr)
	return res
}

func formatEmailTemplate(tmpl, name, eventName, hostName, timeStr, location, googleCal, outlookCal string) string {
	if tmpl == "" {
		tmpl = getDefaultEmailTemplate()
	}
	res := strings.ReplaceAll(tmpl, "[Name]", name)
	res = strings.ReplaceAll(res, "[EventName]", eventName)
	res = strings.ReplaceAll(res, "[HostName]", hostName)
	res = strings.ReplaceAll(res, "[Time]", timeStr)
	res = strings.ReplaceAll(res, "[GoogleCalURL]", googleCal)
	res = strings.ReplaceAll(res, "[OutlookCalURL]", outlookCal)
	if location == "" {
		res = strings.ReplaceAll(res, `<p style="margin: 0;">📍 <b>Location/Link:</b> [Location]</p>`, "")
		res = strings.ReplaceAll(res, `<p style="margin: 0;">📍 <b>Location/Link:</b> </p>`, "")
		res = strings.ReplaceAll(res, `<p>📍 <b>Location/Link:</b> [Location]</p>`, "")
		res = strings.ReplaceAll(res, "[Location]", "")
	} else {
		res = strings.ReplaceAll(res, "[Location]", location)
	}
	return res
}

func startReminderDaemon() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		now := time.Now().UTC()
		rows, err := db.Query(`
			SELECT b.id, b.start_utc, b.name, b.email, b.invitee_tz, e.title, COALESCE(e.location,''), COALESCE(e.description,''), e.duration, e.reminder_minutes, e.email_template, u.username
			FROM bookings b
			JOIN event_types e ON b.event_id = e.id
			JOIN users u ON e.user_id = u.id
			WHERE b.status = 'active' AND b.reminder_sent = 0 AND e.reminder_minutes > 0`)
		if err != nil {
			continue
		}
		for rows.Next() {
			var bID, reminderMins, duration int
			var startUTC, name, email, inviteeTZ, title, loc, desc, tmpl, host string
			rows.Scan(&bID, &startUTC, &name, &email, &inviteeTZ, &title, &loc, &desc, &duration, &reminderMins, &tmpl, &host)
			t, _ := time.Parse(time.RFC3339, startUTC)
			if now.After(t.Add(-time.Duration(reminderMins) * time.Minute)) {
				locTZ := parseLocation(inviteeTZ)
				gURL, oURL := generateCalURLs(title, desc, loc, t, t.Add(time.Duration(duration)*time.Minute))
				body := formatEmailTemplate(tmpl, name, title, host, t.In(locTZ).Format("Mon, Jan 2, 2006 at 3:04 PM"), loc, gURL, oURL)
				sendEmail(email, "Reminder: "+title+" with "+host, body)
				db.Exec("UPDATE bookings SET reminder_sent = 1 WHERE id = ?", bID)
			}
		}
		rows.Close()
	}
}

func render(w http.ResponseWriter, page string, data map[string]interface{}) {
	if data == nil {
		data = make(map[string]interface{})
	}
	data["Page"] = page
	data["BaseURL"] = baseURL
	data["GoogleEnabled"] = googleClientID != ""
	data["MicrosoftEnabled"] = microsoftClientID != ""
	if err := tpl.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, "Template Render Error: "+err.Error(), 500)
	}
}

func handleSignup(w http.ResponseWriter, r *http.Request) {
	username, email, password := r.FormValue("username"), r.FormValue("email"), r.FormValue("password")
	hash, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	_, err := db.Exec("INSERT INTO users (username, email, passhash, timezone, work_days, work_start, work_end) VALUES (?, ?, ?, 'America/New_York', '1,2,3,4,5', '09:00', '17:00')", username, email, string(hash))
	if err != nil {
		render(w, "home", map[string]interface{}{"Error": "Username already taken."})
		return
	}
	handleLogin(w, r)
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	username, password := r.FormValue("username"), r.FormValue("password")
	var id int
	var hash string
	err := db.QueryRow("SELECT id, passhash FROM users WHERE username = ?", username).Scan(&id, &hash)
	if err != nil || bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		render(w, "home", map[string]interface{}{"Error": "Invalid credentials."})
		return
	}
	b := make([]byte, 32)
	rand.Read(b)
	token := hex.EncodeToString(b)
	db.Exec("INSERT INTO sessions (token, user_id, expires) VALUES (?, ?, ?)", token, id, time.Now().Add(72*time.Hour))
	http.SetCookie(w, &http.Cookie{Name: "session", Value: token, Path: "/"})
	http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie("session"); err == nil {
		db.Exec("DELETE FROM sessions WHERE token = ?", c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: "session", Value: "", Path: "/", MaxAge: -1})
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie("session")
		if err != nil {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		var userID int
		err = db.QueryRow("SELECT user_id FROM sessions WHERE token = ? AND expires > ?", c.Value, time.Now()).Scan(&userID)
		if err != nil {
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "", Path: "/", MaxAge: -1})
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		r.Header.Set("X-User-ID", fmt.Sprint(userID))
		next.ServeHTTP(w, r)
	}
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	if _, err := r.Cookie("session"); err == nil {
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
		return
	}
	render(w, "home", nil)
}

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	var u User
	db.QueryRow(`SELECT id, username, COALESCE(email,''), COALESCE(ics_url,''), timezone,
		COALESCE(work_days,''), COALESCE(work_start,'09:00'), COALESCE(work_end,'17:00'),
		COALESCE(google_refresh_token,''), COALESCE(microsoft_refresh_token,''),
		COALESCE(booking_template,''), COALESCE(cancel_template,'')
		FROM users WHERE id = ?`, userID).Scan(
		&u.ID, &u.Username, &u.Email, &u.IcsURL, &u.Timezone,
		&u.WorkDays, &u.WorkStart, &u.WorkEnd,
		&u.GoogleRefreshToken, &u.MicrosoftRefreshToken,
		&u.BookingTemplate, &u.CancelTemplate)

	var events []EventType
	rows, _ := db.Query("SELECT id, title, slug, duration, COALESCE(description,''), COALESCE(reminder_minutes,0) FROM event_types WHERE user_id = ?", userID)
	for rows.Next() {
		var e EventType
		rows.Scan(&e.ID, &e.Title, &e.Slug, &e.Duration, &e.Description, &e.ReminderMinutes)
		events = append(events, e)
	}
	rows.Close()

	var bookings []Booking
	rows, _ = db.Query(`
		SELECT b.id, e.title, b.start_utc, b.name, b.email, COALESCE(b.phone,'')
		FROM bookings b JOIN event_types e ON b.event_id = e.id
		WHERE e.user_id = ? AND b.status = 'active' AND b.start_utc > ?
		ORDER BY b.start_utc ASC LIMIT 50`,
		userID, time.Now().Add(-24*time.Hour).UTC().Format(time.RFC3339))
	for rows.Next() {
		var b Booking
		var utcStr string
		rows.Scan(&b.ID, &b.EventTitle, &utcStr, &b.Name, &b.Email, &b.Phone)
		t, _ := time.Parse(time.RFC3339, utcStr)
		b.StartTime = t.In(parseLocation(u.Timezone)).Format("Mon, Jan 2, 2006 at 3:04 PM")
		bookings = append(bookings, b)
	}
	rows.Close()

	render(w, "dashboard", map[string]interface{}{
		"User": u, "EventTypes": events, "Bookings": bookings,
	})
}

func handleCreateEvent(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	title := r.FormValue("title")
	slug := strings.ToLower(strings.ReplaceAll(title, " ", "-"))
	db.Exec("INSERT INTO event_types (user_id, title, slug, duration, description, location, meeting_type, email_template, reminder_minutes, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
		userID, title, slug, r.FormValue("duration"), r.FormValue("description"),
		r.FormValue("location"), r.FormValue("meeting_type"), r.FormValue("email_template"), r.FormValue("reminder_minutes"))
	http.Redirect(w, r, "/dashboard#event-types", http.StatusSeeOther)
}

func handleDeleteEvent(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	db.Exec("DELETE FROM event_types WHERE id = ? AND user_id = ?", r.FormValue("id"), userID)
	http.Redirect(w, r, "/dashboard#event-types", http.StatusSeeOther)
}

func handleCancelBooking(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	bookingID := r.FormValue("id")

	var inviteeName, inviteeEmail, inviteeTZ, startUTC string
	var eventTitle, eventLocation, hostEmail, hostUsername, hostTZ string
	var googleEventID, microsoftEventID, hostGoogleToken, hostMicrosoftToken string
	var hostCancelTemplate string

	err := db.QueryRow(`
		SELECT b.name, b.email, b.invitee_tz, b.start_utc,
		       e.title, COALESCE(e.location,''),
		       COALESCE(u.email,''), u.username, u.timezone,
		       COALESCE(b.google_event_id,''), COALESCE(b.microsoft_event_id,''),
		       COALESCE(u.google_refresh_token,''), COALESCE(u.microsoft_refresh_token,''),
		       COALESCE(u.cancel_template,'')
		FROM bookings b
		JOIN event_types e ON b.event_id = e.id
		JOIN users u ON e.user_id = u.id
		WHERE b.id = ? AND e.user_id = ?`, bookingID, userID).Scan(
		&inviteeName, &inviteeEmail, &inviteeTZ, &startUTC,
		&eventTitle, &eventLocation, &hostEmail, &hostUsername, &hostTZ,
		&googleEventID, &microsoftEventID, &hostGoogleToken, &hostMicrosoftToken,
		&hostCancelTemplate)

	db.Exec(`UPDATE bookings SET status = 'cancelled' WHERE id = ? AND event_id IN (SELECT id FROM event_types WHERE user_id = ?)`, bookingID, userID)

	if err == nil {
		go func() {
			if googleEventID != "" && hostGoogleToken != "" {
				if token, err := googleRefreshAccessToken(hostGoogleToken); err == nil {
					deleteGoogleEvent(token, googleEventID)
				}
			}
			if microsoftEventID != "" && hostMicrosoftToken != "" {
				if token, err := microsoftRefreshAccessToken(hostMicrosoftToken); err == nil {
					deleteMicrosoftEvent(token, microsoftEventID)
				}
			}
			parsedStart, err := time.Parse(time.RFC3339, startUTC)
			if err != nil {
				return
			}
			inviteeTime := parsedStart.In(parseLocation(inviteeTZ)).Format("Mon, Jan 2, 2006 at 3:04 PM")
			hostTime := parsedStart.In(parseLocation(hostTZ)).Format("Mon, Jan 2, 2006 at 3:04 PM")
			if inviteeEmail != "" {
				sendEmail(inviteeEmail, "Cancelled: "+eventTitle+" with "+hostUsername,
					formatCancelTemplate(hostCancelTemplate, inviteeName, eventTitle, hostUsername, inviteeTime))
			}
			if hostEmail != "" {
				sendEmail(hostEmail, "Booking Cancelled: "+eventTitle+" with "+inviteeName,
					fmt.Sprintf(`<div style="font-family:sans-serif;color:#333;max-width:600px;margin:0 auto;"><h2 style="color:#ef4444;">Booking Cancelled 📅</h2><p><b>Event:</b> %s</p><p><b>Invitee:</b> %s (%s)</p><p><b>Was scheduled for:</b> %s (Your timezone)</p></div>`,
						eventTitle, inviteeName, inviteeEmail, hostTime))
			}
		}()
	}
	http.Redirect(w, r, "/dashboard#scheduled", http.StatusSeeOther)
}

func handleSettings(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	r.ParseForm()
	workDays := strings.Join(r.Form["work_days"], ",")
	if workDays == "" {
		workDays = "1,2,3,4,5"
	}
	db.Exec("UPDATE users SET email = ?, timezone = ?, ics_url = ?, work_days = ?, work_start = ?, work_end = ?, booking_template = ?, cancel_template = ? WHERE id = ?",
		r.FormValue("email"), r.FormValue("timezone"), r.FormValue("ics_url"),
		workDays, r.FormValue("work_start"), r.FormValue("work_end"),
		r.FormValue("booking_template"), r.FormValue("cancel_template"), userID)
	http.Redirect(w, r, "/dashboard#settings", http.StatusSeeOther)
}

func handleGoogleConnect(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	state := hex.EncodeToString([]byte("google:" + userID))
	http.Redirect(w, r, "https://accounts.google.com/o/oauth2/v2/auth?"+url.Values{
		"client_id":     {googleClientID},
		"redirect_uri":  {baseURL + "/auth/google/callback"},
		"response_type": {"code"},
		"scope":         {"https://www.googleapis.com/auth/calendar.events https://www.googleapis.com/auth/calendar.readonly"},
		"access_type":   {"offline"},
		"prompt":        {"consent"},
		"state":         {state},
	}.Encode(), http.StatusSeeOther)
}

func handleGoogleCallback(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		http.Error(w, "Missing code or state", 400)
		return
	}
	resp, err := http.PostForm("https://oauth2.googleapis.com/token", url.Values{
		"code": {code}, "client_id": {googleClientID}, "client_secret": {googleClientSecret},
		"redirect_uri": {baseURL + "/auth/google/callback"}, "grant_type": {"authorization_code"},
	})
	if err != nil {
		http.Error(w, "Token exchange failed", 500)
		return
	}
	defer resp.Body.Close()
	var tok struct {
		RefreshToken string `json:"refresh_token"`
	}
	json.NewDecoder(resp.Body).Decode(&tok)
	if tok.RefreshToken == "" {
		http.Error(w, "No refresh token — revoke app access in Google Account and try again.", 400)
		return
	}
	stateBytes, _ := hex.DecodeString(state)
	parts := strings.SplitN(string(stateBytes), ":", 2)
	if len(parts) != 2 {
		http.Error(w, "Invalid state", 400)
		return
	}
	db.Exec("UPDATE users SET google_refresh_token = ? WHERE id = ?", tok.RefreshToken, parts[1])
	http.Redirect(w, r, "/dashboard#settings", http.StatusSeeOther)
}

func handleGoogleDisconnect(w http.ResponseWriter, r *http.Request) {
	db.Exec("UPDATE users SET google_refresh_token = '' WHERE id = ?", r.Header.Get("X-User-ID"))
	http.Redirect(w, r, "/dashboard#settings", http.StatusSeeOther)
}

func googleRefreshAccessToken(refreshToken string) (string, error) {
	resp, err := http.PostForm("https://oauth2.googleapis.com/token", url.Values{
		"refresh_token": {refreshToken}, "client_id": {googleClientID},
		"client_secret": {googleClientSecret}, "grant_type": {"refresh_token"},
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Error != "" {
		return "", fmt.Errorf("google token refresh: %s", result.Error)
	}
	return result.AccessToken, nil
}

func createGoogleCalendarEvent(accessToken, summary, description, location string, start, end time.Time, inviteeEmail string) (string, string, error) {
	event := map[string]interface{}{
		"summary":     summary,
		"description": description,
		"location":    location,
		"start":       map[string]string{"dateTime": start.UTC().Format(time.RFC3339), "timeZone": "UTC"},
		"end":         map[string]string{"dateTime": end.UTC().Format(time.RFC3339), "timeZone": "UTC"},
		"attendees":   []map[string]string{{"email": inviteeEmail}},
		"conferenceData": map[string]interface{}{
			"createRequest": map[string]interface{}{
				"requestId":             fmt.Sprintf("calpal-%d", time.Now().UnixNano()),
				"conferenceSolutionKey": map[string]string{"type": "hangoutsMeet"},
			},
		},
	}
	body, _ := json.Marshal(event)
	req, _ := http.NewRequestWithContext(context.Background(), "POST",
		"https://www.googleapis.com/calendar/v3/calendars/primary/events?conferenceDataVersion=1&sendUpdates=all",
		bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	var result struct {
		ID             string `json:"id"`
		ConferenceData struct {
			EntryPoints []struct {
				EntryPointType string `json:"entryPointType"`
				URI            string `json:"uri"`
			} `json:"entryPoints"`
		} `json:"conferenceData"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Error.Message != "" {
		return "", "", fmt.Errorf("google calendar: %s", result.Error.Message)
	}
	meetLink := ""
	for _, ep := range result.ConferenceData.EntryPoints {
		if ep.EntryPointType == "video" {
			meetLink = ep.URI
			break
		}
	}
	return result.ID, meetLink, nil
}

func deleteGoogleEvent(accessToken, eventID string) {
	req, err := http.NewRequestWithContext(context.Background(), "DELETE",
		"https://www.googleapis.com/calendar/v3/calendars/primary/events/"+eventID+"?sendUpdates=all", nil)
	if err != nil {
		fmt.Printf("Google delete request build error: %v\n", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Google delete request error: %v\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Google delete failed (status %d): %s\n", resp.StatusCode, string(body))
	}
}

func handleMicrosoftConnect(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	state := hex.EncodeToString([]byte("microsoft:" + userID))
	http.Redirect(w, r, fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/authorize?", microsoftTenantID)+url.Values{
		"client_id":     {microsoftClientID},
		"redirect_uri":  {baseURL + "/auth/microsoft/callback"},
		"response_type": {"code"},
		"scope":         {"offline_access Calendars.ReadWrite OnlineMeetings.ReadWrite"},
		"state":         {state},
	}.Encode(), http.StatusSeeOther)
}

func handleMicrosoftCallback(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		http.Error(w, "Missing code or state", 400)
		return
	}
	resp, err := http.PostForm(
		fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", microsoftTenantID),
		url.Values{
			"code": {code}, "client_id": {microsoftClientID}, "client_secret": {microsoftClientSecret},
			"redirect_uri": {baseURL + "/auth/microsoft/callback"}, "grant_type": {"authorization_code"},
		})
	if err != nil {
		http.Error(w, "Token exchange failed", 500)
		return
	}
	defer resp.Body.Close()
	var tok struct {
		RefreshToken string `json:"refresh_token"`
	}
	json.NewDecoder(resp.Body).Decode(&tok)
	if tok.RefreshToken == "" {
		http.Error(w, "No refresh token returned.", 400)
		return
	}
	stateBytes, _ := hex.DecodeString(state)
	parts := strings.SplitN(string(stateBytes), ":", 2)
	if len(parts) != 2 {
		http.Error(w, "Invalid state", 400)
		return
	}
	db.Exec("UPDATE users SET microsoft_refresh_token = ? WHERE id = ?", tok.RefreshToken, parts[1])
	http.Redirect(w, r, "/dashboard#settings", http.StatusSeeOther)
}

func handleMicrosoftDisconnect(w http.ResponseWriter, r *http.Request) {
	db.Exec("UPDATE users SET microsoft_refresh_token = '' WHERE id = ?", r.Header.Get("X-User-ID"))
	http.Redirect(w, r, "/dashboard#settings", http.StatusSeeOther)
}

func microsoftRefreshAccessToken(refreshToken string) (string, error) {
	resp, err := http.PostForm(
		fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", microsoftTenantID),
		url.Values{
			"refresh_token": {refreshToken}, "client_id": {microsoftClientID},
			"client_secret": {microsoftClientSecret}, "grant_type": {"refresh_token"},
			"scope": {"offline_access Calendars.ReadWrite OnlineMeetings.ReadWrite"},
		})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Error != "" {
		return "", fmt.Errorf("microsoft token refresh: %s", result.Error)
	}
	return result.AccessToken, nil
}

func createMicrosoftCalendarEvent(accessToken, summary, description, location string, start, end time.Time, inviteeEmail string) (string, string, error) {
	event := map[string]interface{}{
		"subject":  summary,
		"body":     map[string]string{"contentType": "text", "content": description},
		"location": map[string]string{"displayName": location},
		"start":    map[string]string{"dateTime": start.UTC().Format("2006-01-02T15:04:05"), "timeZone": "UTC"},
		"end":      map[string]string{"dateTime": end.UTC().Format("2006-01-02T15:04:05"), "timeZone": "UTC"},
		"attendees": []map[string]interface{}{
			{"emailAddress": map[string]string{"address": inviteeEmail}, "type": "required"},
		},
		"isOnlineMeeting":       true,
		"onlineMeetingProvider": "teamsForBusiness",
	}
	body, _ := json.Marshal(event)
	req, _ := http.NewRequestWithContext(context.Background(), "POST",
		"https://graph.microsoft.com/v1.0/me/events", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	var result struct {
		ID            string `json:"id"`
		OnlineMeeting struct {
			JoinURL string `json:"joinUrl"`
		} `json:"onlineMeeting"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	json.Unmarshal(respBody, &result)
	if result.Error.Message != "" {
		return "", "", fmt.Errorf("microsoft calendar: %s", result.Error.Message)
	}
	return result.ID, result.OnlineMeeting.JoinURL, nil
}

func deleteMicrosoftEvent(accessToken, eventID string) {
	req, err := http.NewRequestWithContext(context.Background(), "DELETE",
		"https://graph.microsoft.com/v1.0/me/events/"+eventID, nil)
	if err != nil {
		fmt.Printf("Microsoft delete request build error: %v\n", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Microsoft delete request error: %v\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Microsoft delete failed (status %d): %s\n", resp.StatusCode, string(body))
	}
}

func fetchGoogleBusyTimes(refreshToken string, dayStart, dayEnd time.Time) []struct{ Start, End time.Time } {
	accessToken, err := googleRefreshAccessToken(refreshToken)
	if err != nil {
		fmt.Printf("Google busy-time token refresh error: %v\n", err)
		return nil
	}

	body, _ := json.Marshal(map[string]interface{}{
		"timeMin": dayStart.UTC().Format(time.RFC3339),
		"timeMax": dayEnd.UTC().Format(time.RFC3339),
		"items":   []map[string]string{{"id": "primary"}},
	})
	req, _ := http.NewRequestWithContext(context.Background(), "POST",
		"https://www.googleapis.com/calendar/v3/freeBusy",
		bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Google freeBusy error: %v\n", err)
		return nil
	}
	defer resp.Body.Close()

	var result struct {
		Calendars map[string]struct {
			Busy []struct {
				Start string `json:"start"`
				End   string `json:"end"`
			} `json:"busy"`
		} `json:"calendars"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	var busy []struct{ Start, End time.Time }
	for _, cal := range result.Calendars {
		for _, b := range cal.Busy {
			s, err1 := time.Parse(time.RFC3339, b.Start)
			e, err2 := time.Parse(time.RFC3339, b.End)
			if err1 == nil && err2 == nil {
				busy = append(busy, struct{ Start, End time.Time }{s, e})
			}
		}
	}
	return busy
}

func fetchMicrosoftBusyTimes(refreshToken string, dayStart, dayEnd time.Time) []struct{ Start, End time.Time } {
	accessToken, err := microsoftRefreshAccessToken(refreshToken)
	if err != nil {
		fmt.Printf("Microsoft busy-time token refresh error: %v\n", err)
		return nil
	}

	body, _ := json.Marshal(map[string]interface{}{
		"schedules":                []string{"me"},
		"startTime":                map[string]string{"dateTime": dayStart.UTC().Format("2006-01-02T15:04:05"), "timeZone": "UTC"},
		"endTime":                  map[string]string{"dateTime": dayEnd.UTC().Format("2006-01-02T15:04:05"), "timeZone": "UTC"},
		"availabilityViewInterval": 15,
	})
	req, _ := http.NewRequestWithContext(context.Background(), "POST",
		"https://graph.microsoft.com/v1.0/me/calendar/getSchedule",
		bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Microsoft getSchedule error: %v\n", err)
		return nil
	}
	defer resp.Body.Close()

	var result struct {
		Value []struct {
			ScheduleItems []struct {
				Start struct {
					DateTime string `json:"dateTime"`
					TimeZone string `json:"timeZone"`
				} `json:"start"`
				End struct {
					DateTime string `json:"dateTime"`
					TimeZone string `json:"timeZone"`
				} `json:"end"`
			} `json:"scheduleItems"`
		} `json:"value"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	var busy []struct{ Start, End time.Time }
	for _, sched := range result.Value {
		for _, item := range sched.ScheduleItems {
			s, err1 := time.Parse("2006-01-02T15:04:05.0000000", item.Start.DateTime)
			e, err2 := time.Parse("2006-01-02T15:04:05.0000000", item.End.DateTime)
			if err1 != nil {
				s, err1 = time.Parse("2006-01-02T15:04:05", item.Start.DateTime)
				e, err2 = time.Parse("2006-01-02T15:04:05", item.End.DateTime)
			}
			if err1 == nil && err2 == nil {
				s = s.UTC()
				e = e.UTC()
				busy = append(busy, struct{ Start, End time.Time }{s, e})
			}
		}
	}
	return busy
}

func handlePublicPage(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/u/"), "/")
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	username, slug := parts[0], parts[1]

	var host User
	err := db.QueryRow(`SELECT id, username, COALESCE(ics_url,''), timezone,
		COALESCE(work_days,''), COALESCE(work_start,'09:00'), COALESCE(work_end,'17:00'),
		COALESCE(google_refresh_token,''), COALESCE(microsoft_refresh_token,'')
		FROM users WHERE username = ?`, username).Scan(
		&host.ID, &host.Username, &host.IcsURL, &host.Timezone,
		&host.WorkDays, &host.WorkStart, &host.WorkEnd,
		&host.GoogleRefreshToken, &host.MicrosoftRefreshToken)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	var evt EventType
	err = db.QueryRow(`SELECT id, title, duration, COALESCE(description,''), COALESCE(location,''), COALESCE(meeting_type,'custom_link'), is_active
		FROM event_types WHERE user_id = ? AND slug = ?`, host.ID, slug).Scan(
		&evt.ID, &evt.Title, &evt.Duration, &evt.Description, &evt.Location, &evt.MeetingType, &evt.IsActive)
	if err != nil || !evt.IsActive {
		http.NotFound(w, r)
		return
	}

	visitorTZ := r.URL.Query().Get("tz")
	visitorLoc := parseLocation(visitorTZ)
	hostLoc := parseLocation(host.Timezone)

	dateStr := r.URL.Query().Get("d")
	if dateStr == "" {
		dateStr = time.Now().In(hostLoc).Format("2006-01-02")
	}
	targetDateHost, _ := time.ParseInLocation("2006-01-02", dateStr, hostLoc)

	type SlotDisplay struct {
		UTCStart    string
		DisplayTime string
		Available   bool
	}
	var slots []SlotDisplay

	extEvents := fetchICS(host.ID, host.IcsURL)

	type BusyInterval struct{ Start, End time.Time }
	var calBusy []BusyInterval

	dayStart := time.Date(targetDateHost.Year(), targetDateHost.Month(), targetDateHost.Day(), 0, 0, 0, 0, hostLoc).UTC()
	dayEnd := dayStart.Add(24 * time.Hour)

	if host.GoogleRefreshToken != "" {
		for _, b := range fetchGoogleBusyTimes(host.GoogleRefreshToken, dayStart, dayEnd) {
			calBusy = append(calBusy, BusyInterval{b.Start, b.End})
		}
	}
	if host.MicrosoftRefreshToken != "" {
		for _, b := range fetchMicrosoftBusyTimes(host.MicrosoftRefreshToken, dayStart, dayEnd) {
			calBusy = append(calBusy, BusyInterval{b.Start, b.End})
		}
	}

	type LocalBooking struct{ Start, End time.Time }
	var localBookings []LocalBooking
	rows, _ := db.Query(`SELECT b.start_utc, e.duration FROM bookings b JOIN event_types e ON b.event_id = e.id WHERE b.status = 'active' AND e.user_id = ?`, host.ID)
	for rows.Next() {
		var u string
		var dur int
		rows.Scan(&u, &dur)
		t, _ := time.Parse(time.RFC3339, u)
		localBookings = append(localBookings, LocalBooking{t, t.Add(time.Duration(dur) * time.Minute)})
	}
	rows.Close()

	startH, startM, sErr := parseTimeStr(host.WorkStart)
	endH, endM, eErr := parseTimeStr(host.WorkEnd)
	if sErr != nil {
		startH, startM = 9, 0
	}
	if eErr != nil {
		endH, endM = 17, 0
	}

	dayNum := fmt.Sprintf("%d", int(targetDateHost.Weekday()))
	if strings.Contains(host.WorkDays, dayNum) {
		startOfDay := time.Date(targetDateHost.Year(), targetDateHost.Month(), targetDateHost.Day(), startH, startM, 0, 0, hostLoc)
		endOfDay := time.Date(targetDateHost.Year(), targetDateHost.Month(), targetDateHost.Day(), endH, endM, 0, 0, hostLoc)

		for t := startOfDay; t.Before(endOfDay); t = t.Add(time.Duration(evt.Duration) * time.Minute) {
			slotEnd := t.Add(time.Duration(evt.Duration) * time.Minute)
			if slotEnd.After(endOfDay) || t.Before(time.Now()) {
				continue
			}
			utcStr := t.UTC().Format(time.RFC3339)
			isAvail := true
			for _, b := range localBookings {
				if t.Before(b.End) && slotEnd.After(b.Start) {
					isAvail = false
					break
				}
			}
			if isAvail {
				for _, ext := range extEvents {
					if t.Before(*ext.End) && slotEnd.After(*ext.Start) {
						isAvail = false
						break
					}
				}
			}
			if isAvail {
				for _, b := range calBusy {
					if t.Before(b.End) && slotEnd.After(b.Start) {
						isAvail = false
						break
					}
				}
			}
			slots = append(slots, SlotDisplay{UTCStart: utcStr, DisplayTime: utcStr, Available: isAvail})
		}
	}

	render(w, "booking", map[string]interface{}{
		"Host": host, "EventType": evt,
		"VisitorTZ":          visitorLoc.String(),
		"CurrentDate":        targetDateHost.Format("2006-01-02"),
		"CurrentDateDisplay": targetDateHost.Format("Mon, Jan 02, 2006"),
		"PrevDate":           targetDateHost.AddDate(0, 0, -1).Format("2006-01-02"),
		"NextDate":           targetDateHost.AddDate(0, 0, 1).Format("2006-01-02"),
		"Slots":              slots,
		"Message":            r.URL.Query().Get("msg"),
		"MeetingType":        evt.MeetingType,
	})
}

func handlePublicBooking(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/u/"), "/")
	username, slug := parts[0], parts[1]

	utcStart := r.FormValue("utc_start")
	name := r.FormValue("name")
	inviteeEmail := r.FormValue("email")
	phone := r.FormValue("phone")
	tz := r.FormValue("tz")

	var host User
	db.QueryRow(`SELECT id, username, email, timezone, COALESCE(google_refresh_token,''), COALESCE(microsoft_refresh_token,''), COALESCE(booking_template,'') FROM users WHERE username = ?`, username).Scan(
		&host.ID, &host.Username, &host.Email, &host.Timezone, &host.GoogleRefreshToken, &host.MicrosoftRefreshToken, &host.BookingTemplate)

	var evt EventType
	db.QueryRow("SELECT id, title, duration, location, description, COALESCE(meeting_type,'custom_link'), email_template FROM event_types WHERE user_id = ? AND slug = ?", host.ID, slug).Scan(
		&evt.ID, &evt.Title, &evt.Duration, &evt.Location, &evt.Description, &evt.MeetingType, &evt.EmailTemplate)

	parsedStart, _ := time.Parse(time.RFC3339, utcStart)
	parsedEnd := parsedStart.Add(time.Duration(evt.Duration) * time.Minute)

	var hasConflict bool
	rows, _ := db.Query("SELECT b.start_utc, e.duration FROM bookings b JOIN event_types e ON b.event_id = e.id WHERE b.status = 'active' AND e.user_id = ?", host.ID)
	for rows.Next() {
		var s string
		var d int
		rows.Scan(&s, &d)
		t, _ := time.Parse(time.RFC3339, s)
		if parsedStart.Before(t.Add(time.Duration(d)*time.Minute)) && parsedEnd.After(t) {
			hasConflict = true
			break
		}
	}
	rows.Close()
	if hasConflict {
		http.Error(w, "Slot taken! Please go back and select another time.", 409)
		return
	}

	inviteePhone := r.FormValue("invitee_phone")
	result, _ := db.Exec("INSERT INTO bookings (event_id, start_utc, name, email, phone, status, invitee_tz, invitee_phone) VALUES (?, ?, ?, ?, ?, 'active', ?, ?)",
		evt.ID, utcStart, name, inviteeEmail, phone, tz, inviteePhone)
	bookingID, _ := result.LastInsertId()

	go func() {
		inviteeTime := parsedStart.In(parseLocation(tz)).Format("Mon, Jan 2, 2006 at 3:04 PM")
		hostTime := parsedStart.In(parseLocation(host.Timezone)).Format("Mon, Jan 2, 2006 at 3:04 PM")

		meetLink := ""

		switch evt.MeetingType {
		case "google_meet":
			if host.GoogleRefreshToken != "" {
				if token, err := googleRefreshAccessToken(host.GoogleRefreshToken); err == nil {
					if eventID, link, err := createGoogleCalendarEvent(token, evt.Title, evt.Description, "", parsedStart, parsedEnd, inviteeEmail); err == nil {
						db.Exec("UPDATE bookings SET google_event_id = ? WHERE id = ?", eventID, bookingID)
						meetLink = link
					} else {
						fmt.Printf("Google Calendar error: %v\n", err)
					}
				}
			}

		case "teams":
			if host.MicrosoftRefreshToken != "" {
				if token, err := microsoftRefreshAccessToken(host.MicrosoftRefreshToken); err == nil {
					if eventID, link, err := createMicrosoftCalendarEvent(token, evt.Title, evt.Description, "", parsedStart, parsedEnd, inviteeEmail); err == nil {
						db.Exec("UPDATE bookings SET microsoft_event_id = ? WHERE id = ?", eventID, bookingID)
						meetLink = link
					} else {
						fmt.Printf("Microsoft Calendar error: %v\n", err)
					}
				}
			}

		case "phone":
			meetLink = ""

		case "custom_link", "in_person":
			meetLink = evt.Location
		}

		bookingTmpl := evt.EmailTemplate
		if bookingTmpl == "" {
			bookingTmpl = host.BookingTemplate
		}

		icsUID := fmt.Sprintf("calpal-%d@calpal", bookingID)
		icsLocation := meetLink
		if evt.MeetingType == "in_person" {
			icsLocation = evt.Location
		} else if evt.MeetingType == "phone" {
			icsLocation = fmt.Sprintf("Phone call — %s will call you", host.Username)
		}
		icsContent := buildICS(icsUID, evt.Title, evt.Description, icsLocation, parsedStart, parsedEnd, host.Email, inviteeEmail)

		var inviteeBody string

		gURL, oURL := generateCalURLs(evt.Title, evt.Description, meetLink, parsedStart, parsedEnd)

		switch evt.MeetingType {
		case "phone":
			inviteeBody = formatEmailTemplate(bookingTmpl, name, evt.Title, host.Username, inviteeTime,
				fmt.Sprintf("📞 Phone call — %s will call you at %s", host.Username, inviteePhone), gURL, oURL)
		case "in_person":
			inviteeBody = formatEmailTemplate(bookingTmpl, name, evt.Title, host.Username, inviteeTime,
				fmt.Sprintf("📍 In person at %s", evt.Location), gURL, oURL)
		default:
			inviteeBody = formatEmailTemplate(bookingTmpl, name, evt.Title, host.Username, inviteeTime, meetLink, gURL, oURL)
		}
		sendEmailWithICS(inviteeEmail, "Confirmed: "+evt.Title+" with "+host.Username, inviteeBody, icsContent, "invite.ics")

		if host.Email != "" {
			var locationLine string
			switch evt.MeetingType {
			case "phone":
				locationLine = fmt.Sprintf("<p><b>📞 Phone:</b> Call invitee at %s</p>", inviteePhone)
			case "google_meet", "teams":
				locationLine = fmt.Sprintf("<p><b>🔗 Meeting Link:</b> <a href=\"%s\">%s</a></p>", meetLink, meetLink)
			case "in_person":
				locationLine = fmt.Sprintf("<p><b>📍 Location:</b> %s</p>", evt.Location)
			default:
				if meetLink != "" {
					locationLine = fmt.Sprintf("<p><b>🔗 Link:</b> <a href=\"%s\">%s</a></p>", meetLink, meetLink)
				}
			}
			sendEmailWithICS(host.Email, "New Booking: "+evt.Title+" with "+name,
				fmt.Sprintf(`<h3>You have a new booking! 📅</h3><p><b>Event:</b> %s</p><p><b>Name:</b> %s</p><p><b>Email:</b> %s</p><p><b>Time:</b> %s (Your timezone)</p>%s`,
					evt.Title, name, inviteeEmail, hostTime, locationLine),
				icsContent, "invite.ics")
		}
	}()

	http.Redirect(w, r, fmt.Sprintf("/u/%s/%s?msg=Booking+Confirmed!+Check+your+email.&tz=%s", username, slug, tz), http.StatusSeeOther)
}

func parseTimeStr(t string) (int, int, error) {
	if strings.TrimSpace(t) == "" {
		return 0, 0, fmt.Errorf("empty time string")
	}
	var h, m int
	_, err := fmt.Sscanf(t, "%d:%d", &h, &m)
	return h, m, err
}

func fetchICS(userID int, url string) []gocal.Event {
	if url == "" {
		return nil
	}
	icsMutex.Lock()
	defer icsMutex.Unlock()
	resp, err := http.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	c := gocal.NewParser(resp.Body)
	start, end := time.Now().AddDate(0, -1, 0), time.Now().AddDate(0, 3, 0)
	c.Start, c.End = &start, &end
	c.Parse()
	var events []gocal.Event
	for _, e := range c.Events {
		us, ue := e.Start.UTC(), e.End.UTC()
		e.Start, e.End = &us, &ue
		events = append(events, e)
	}
	return events
}
