package exitmanager

// import (
// 	"fmt"
// 	"log"
// 	"time"

// 	exitmanager "github.com/mcwalrus/exit-manager"
// )

// // Example demonstrating panic handling in exit-manager
// func main() {
// 	em := exitmanager.Global()
// 	httpEM := em.RegisterHTTPExitManager()

// 	// Register cleanup functions that might panic
// 	em.RegisterCleanup(func() {
// 		log.Println("Normal cleanup function")
// 	})

// 	em.RegisterCleanup(func() {
// 		log.Println("Cleanup function that will panic!")
// 		panic("cleanup panic - but shutdown continues gracefully")
// 	})

// 	em.RegisterCleanup(func() {
// 		log.Println("Another normal cleanup function")
// 	})

// 	// Register HTTP pre-shutdown that might panic
// 	httpEM.RegisterPreShutdown(func() {
// 		log.Println("Pre-shutdown hook that will panic!")
// 		panic("pre-shutdown panic - but shutdown continues gracefully")
// 	})

// 	httpEM.RegisterPreShutdown(func() {
// 		log.Println("Normal pre-shutdown hook")
// 	})

// 	// Example of WithShutdownLock with panic
// 	go func() {
// 		time.Sleep(100 * time.Millisecond)

// 		defer func() {
// 			if r := recover(); r != nil {
// 				log.Printf("Caught panic from WithShutdownLock: %v", r)
// 			}
// 		}()

// 		err := em.WithShutdownLock(func() error {
// 			log.Println("Function with shutdown lock that will panic!")
// 			panic("WithShutdownLock panic - lock still gets released")
// 		})

// 		// This won't be reached due to panic
// 		log.Printf("WithShutdownLock returned: %v", err)
// 	}()

// 	// Simulate some work
// 	go func() {
// 		time.Sleep(200 * time.Millisecond)
// 		log.Println("Initiating shutdown...")
// 		em.Shutdown()
// 	}()

// 	// Wait for shutdown
// 	<-em.Notify()
// 	fmt.Println("\nShutdown completed successfully despite panics!")
// 	fmt.Println("All user-registered functions executed, with panics recovered gracefully.")
// }
