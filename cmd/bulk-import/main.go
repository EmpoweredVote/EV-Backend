package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/EmpoweredVote/EV-Backend/internal/db"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials"
	"github.com/EmpoweredVote/EV-Backend/internal/essentials/cicero"
	"github.com/joho/godotenv"
)

// LA County ZIP codes — delivery-address ZIPs (excludes PO-box-only and
// military ZIPs). Covers Downtown, Westside, South Bay, San Fernando Valley,
// San Gabriel Valley, Antelope Valley, and gateway cities.
var laCountyZips = []string{
	// Central / Downtown LA
	"90001", "90002", "90003", "90004", "90005", "90006", "90007", "90008",
	"90010", "90011", "90012", "90013", "90014", "90015", "90016", "90017",
	"90018", "90019", "90020", "90021", "90022", "90023", "90024", "90025",
	"90026", "90027", "90028", "90029", "90031", "90032", "90033", "90034",
	"90035", "90036", "90037", "90038", "90039", "90040", "90041", "90042",
	"90043", "90044", "90045", "90046", "90047", "90048", "90049", "90056",
	"90057", "90058", "90059", "90061", "90062", "90063", "90064", "90065",
	"90066", "90067", "90068", "90069", "90071", "90077", "90089",
	// Westside / Santa Monica / Beverly Hills / Culver City
	"90201", "90210", "90211", "90212", "90220", "90221", "90222",
	"90230", "90232", "90240", "90241", "90242", "90245", "90247",
	"90248", "90249", "90250", "90254", "90255", "90260", "90262",
	"90263", "90265", "90266", "90270", "90272", "90274", "90275",
	"90277", "90278", "90280", "90290", "90291", "90292", "90293",
	"90301", "90302", "90303", "90304", "90305",
	// Torrance / Carson / Harbor
	"90501", "90502", "90503", "90504", "90505", "90506",
	// Whittier / Norwalk / La Mirada / Pico Rivera
	"90601", "90602", "90603", "90604", "90605", "90606",
	"90631", "90638", "90639", "90640", "90650", "90660", "90670",
	// Long Beach / Lakewood / Signal Hill / Cerritos
	"90701", "90703", "90706", "90710", "90712", "90713",
	"90715", "90716", "90717", "90720", "90723",
	"90731", "90732", "90740", "90742", "90743", "90744", "90745",
	"90746", "90747", "90755",
	"90802", "90803", "90804", "90805", "90806", "90807", "90808",
	"90810", "90813", "90814", "90815",
	// Pasadena / San Gabriel Valley
	"91001", "91006", "91007", "91008", "91010", "91011", "91016",
	"91020", "91024", "91030",
	"91040", "91042", "91043",
	"91101", "91103", "91104", "91105", "91106", "91107", "91108",
	// Glendale / Burbank
	"91201", "91202", "91203", "91204", "91205", "91206", "91207", "91208",
	"91214",
	// San Fernando Valley — West
	"91301", "91302", "91303", "91304", "91306", "91307", "91311",
	"91316", "91324", "91325", "91326", "91330", "91331", "91335",
	"91340", "91342", "91343", "91344", "91345",
	// Santa Clarita / Castaic / Antelope Valley
	"91321", "91350", "91351", "91354", "91355", "91381", "91384",
	"91387", "91390",
	"93510", "93534", "93535", "93536", "93543", "93544", "93550",
	"93551", "93552", "93553", "93560", "93563", "93591",
	// San Fernando Valley — East / North Hollywood / Sherman Oaks
	"91352", "91356", "91364", "91367",
	"91401", "91402", "91403", "91405", "91406", "91411", "91423", "91436",
	// Burbank / Studio City / North Hollywood
	"91501", "91502", "91504", "91505", "91506",
	"91601", "91602", "91604", "91605", "91606", "91607",
	// East LA / West Covina / Pomona / Covina
	"91702", "91706", "91709", "91710", "91711",
	"91722", "91723", "91724",
	"91731", "91732", "91733",
	"91740", "91741", "91744", "91745", "91746", "91748", "91750",
	"91754", "91755",
	"91765", "91766", "91767", "91768",
	"91770", "91773", "91775", "91776", "91780",
	"91789", "91790", "91791", "91792",
	// Alhambra / Arcadia / Monrovia / Azusa
	"91801", "91803",
}

func main() {
	_ = godotenv.Load(".env.local")

	delayMs := 3000
	log.SetFlags(log.Ldate | log.Ltime)

	// Build a Cicero provider from the env key
	ciceroKey := os.Getenv("CICERO_KEY")
	if ciceroKey == "" {
		log.Fatal("CICERO_KEY not set in environment / .env.local")
	}

	fmt.Println("=========================================")
	fmt.Println("LA County Bulk Import (Cicero → DB)")
	fmt.Println("=========================================")
	fmt.Printf("Provider:  Cicero (seeding only)\n")
	fmt.Printf("ZIP codes: %d\n", len(laCountyZips))
	fmt.Printf("Delay:     %dms between ZIPs\n", delayMs)
	fmt.Printf("Est. time: ~%d minutes\n", (len(laCountyZips)*delayMs/1000)/60)
	fmt.Println()

	// Connect to database and run migrations
	db.Connect()
	essentials.Init() // sets up schema + tables (Provider will be ballotready from env)

	// Create a Cicero provider just for this import
	ciceroProvider := cicero.NewProvider(ciceroKey)

	// Handle Ctrl+C gracefully
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt)
	cancelled := false
	go func() {
		<-done
		fmt.Println("\nReceived interrupt — finishing current ZIP then stopping...")
		cancelled = true
	}()

	delay := time.Duration(delayMs) * time.Millisecond
	completed := 0
	failed := 0
	var failedZips []string
	start := time.Now()

	fmt.Println("Starting import...")
	fmt.Println()

	for i, zip := range laCountyZips {
		if cancelled {
			fmt.Printf("\nStopped at ZIP %d/%d\n", i, len(laCountyZips))
			break
		}

		fmt.Printf("[%d/%d] ZIP %s ...", i+1, len(laCountyZips), zip)

		if err := essentials.WarmZipWith(ciceroProvider, zip); err != nil {
			fmt.Printf(" FAILED: %v\n", err)
			failed++
			failedZips = append(failedZips, zip)
		} else {
			fmt.Printf(" OK\n")
			completed++
		}

		// Delay between ZIPs (skip after last)
		if i < len(laCountyZips)-1 && !cancelled {
			time.Sleep(delay)
		}
	}

	elapsed := time.Since(start)
	fmt.Println()
	fmt.Println("=========================================")
	fmt.Println("Import Complete")
	fmt.Println("=========================================")
	fmt.Printf("Completed: %d\n", completed)
	fmt.Printf("Failed:    %d\n", failed)
	fmt.Printf("Elapsed:   %s\n", elapsed.Round(time.Second))

	if len(failedZips) > 0 {
		fmt.Printf("\nFailed ZIPs: %v\n", failedZips)
	}
}
