package main

import (
	"fmt"
	"os"

	"gobase/buffer_pool_manager"
	"gobase/disk_manager"
	"gobase/shared"
	"gobase/slotted_page"
)

func main() {
	// Supprimer le fichier de test s'il existe
	os.Remove("test.db")

	fmt.Println("=== TEST DISK MANAGER ===")
	testDiskManager()

	fmt.Println("\n=== TEST BUFFER POOL MANAGER ===")
	testBufferPoolManager()

	fmt.Println("\n=== TEST SLOTTED PAGE (standalone) ===")
	testSlottedPageStandalone()

	fmt.Println("\n=== TEST INTEGRATION COMPLETE ===")
	testIntegration()

	// Nettoyage
	os.Remove("test.db")
	fmt.Println("\nTous les tests sont terminés!")
}

func testDiskManager() {
	dm, err := disk_manager.NewDiskManager("test.db")
	if err != nil {
		fmt.Printf("ERREUR création DiskManager: %v\n", err)
		return
	}
	fmt.Println("1. DiskManager créé")

	pageID, err := dm.AllocatePage()
	if err != nil {
		fmt.Printf("ERREUR allocation: %v\n", err)
		return
	}
	fmt.Printf("2. Page allouée avec ID: %d\n", pageID)

	data := make([]byte, shared.PAGE_SIZE)
	copy(data, []byte("Hello DiskManager!"))
	err = dm.WritePage(pageID, data)
	if err != nil {
		fmt.Printf("ERREUR écriture: %v\n", err)
		return
	}
	fmt.Println("3. Données écrites sur la page")

	dm.Close()
	fmt.Println("4. DiskManager fermé")

	dm2, err := disk_manager.NewDiskManager("test.db")
	if err != nil {
		fmt.Printf("ERREUR réouverture: %v\n", err)
		return
	}

	readData, err := dm2.ReadPage(pageID)
	if err != nil {
		fmt.Printf("ERREUR lecture: %v\n", err)
		return
	}

	fmt.Printf("5. Données relues après réouverture: %s\n", string(readData[:18]))
	dm2.Close()
}

func testBufferPoolManager() {
	os.Remove("test.db")

	dm, _ := disk_manager.NewDiskManager("test.db")
	bpm := buffer_pool_manager.NewBufferPoolManager(dm, 3)
	fmt.Println("1. BufferPoolManager créé avec poolSize=3")

	pageID1, page1, err := bpm.NewPage()
	if err != nil {
		fmt.Printf("ERREUR NewPage: %v\n", err)
		return
	}
	copy(page1.Data, []byte("Page 1 - Bonjour!"))
	fmt.Printf("2. Nouvelle page créée avec ID: %d\n", pageID1)

	bpm.UnpinPage(pageID1, true)
	bpm.FlushPage(pageID1)
	fmt.Println("3. Page flushée sur disque")

	dm.Close()
	dm2, _ := disk_manager.NewDiskManager("test.db")
	readData, _ := dm2.ReadPage(pageID1)
	fmt.Printf("4. Vérification persistance: %s\n", string(readData[:17]))
	dm2.Close()
}

func testSlottedPageStandalone() {
	// Test SlottedPage sans BufferPool (en mémoire seulement)
	sp := slotted_page.NewSlottedPage()
	fmt.Printf("1. SlottedPage créée, espace libre: %d octets\n", sp.GetFreeSpace())

	// Insérer des tuples
	tuple1 := shared.NewTuple("Alice,30,Paris")
	slotID1, err := sp.InsertTuple(tuple1)
	if err != nil {
		fmt.Printf("ERREUR InsertTuple: %v\n", err)
		return
	}
	fmt.Printf("2. Tuple 1 inséré au slot %d: %s\n", slotID1, string(tuple1))

	tuple2 := shared.NewTuple("Bob,25,Lyon")
	slotID2, err := sp.InsertTuple(tuple2)
	if err != nil {
		fmt.Printf("ERREUR InsertTuple: %v\n", err)
		return
	}
	fmt.Printf("3. Tuple 2 inséré au slot %d: %s\n", slotID2, string(tuple2))

	fmt.Printf("4. Espace libre restant: %d octets\n", sp.GetFreeSpace())

	// Lire les tuples
	readTuple1, _ := sp.GetTuple(slotID1)
	fmt.Printf("5. Lecture slot %d: %s\n", slotID1, string(readTuple1))

	readTuple2, _ := sp.GetTuple(slotID2)
	fmt.Printf("6. Lecture slot %d: %s\n", slotID2, string(readTuple2))

	// Supprimer un tuple
	sp.DeleteTuple(slotID1)
	fmt.Printf("7. Tuple au slot %d supprimé\n", slotID1)

	// Essayer de lire le tuple supprimé
	_, err = sp.GetTuple(slotID1)
	if err != nil {
		fmt.Printf("8. Lecture slot %d après suppression: %v (attendu)\n", slotID1, err)
	}

	// Le tuple 2 est toujours accessible
	readTuple2Again, _ := sp.GetTuple(slotID2)
	fmt.Printf("9. Tuple 2 toujours accessible: %s\n", string(readTuple2Again))
}

func testIntegration() {
	// Test complet : SlottedPage + BufferPool + DiskManager
	os.Remove("test.db")

	// === PHASE 1 : Créer et remplir une page ===
	fmt.Println("--- Phase 1 : Création et insertion ---")

	dm, _ := disk_manager.NewDiskManager("test.db")
	bpm := buffer_pool_manager.NewBufferPoolManager(dm, 3)

	// Créer une nouvelle page via BufferPool
	pageID, page, _ := bpm.NewPage()
	fmt.Printf("1. Page %d créée via BufferPool\n", pageID)

	// Initialiser cette page comme SlottedPage
	// On écrit le header directement dans page.Data
	sp := slotted_page.FromData(page.Data)

	// Initialiser le header (comme NewSlottedPage mais sur page.Data)
	// Note: page.Data est déjà alloué par BufferPool, on doit l'initialiser
	initSlottedPage(page.Data)

	// Insérer des tuples
	sp.InsertTuple(shared.NewTuple("User1,Alice,30"))
	sp.InsertTuple(shared.NewTuple("User2,Bob,25"))
	sp.InsertTuple(shared.NewTuple("User3,Charlie,35"))
	fmt.Println("2. 3 tuples insérés dans la page")

	// Marquer comme dirty et flush
	bpm.UnpinPage(pageID, true)
	bpm.FlushPage(pageID)
	fmt.Println("3. Page flushée sur disque")

	dm.Close()

	// === PHASE 2 : Relire depuis le disque ===
	fmt.Println("--- Phase 2 : Relecture depuis disque ---")

	dm2, _ := disk_manager.NewDiskManager("test.db")
	bpm2 := buffer_pool_manager.NewBufferPoolManager(dm2, 3)

	// Récupérer la page
	page2, _ := bpm2.FetchPage(pageID)
	fmt.Printf("4. Page %d récupérée depuis le disque\n", pageID)

	// Interpréter comme SlottedPage
	sp2 := slotted_page.FromData(page2.Data)

	// Lire les tuples
	tuple0, _ := sp2.GetTuple(0)
	tuple1, _ := sp2.GetTuple(1)
	tuple2, _ := sp2.GetTuple(2)

	fmt.Printf("5. Tuple 0: %s\n", string(tuple0))
	fmt.Printf("6. Tuple 1: %s\n", string(tuple1))
	fmt.Printf("7. Tuple 2: %s\n", string(tuple2))

	bpm2.UnpinPage(pageID, false)
	dm2.Close()

	fmt.Println("8. Intégration complète réussie!")
}

// Helper pour initialiser un []byte comme SlottedPage
func initSlottedPage(data []byte) {
	// numSlots = 0
	data[0] = 0
	data[1] = 0
	// freeSpaceEnd = 4096 (little endian: 0x1000 = [0x00, 0x10])
	data[2] = 0x00
	data[3] = 0x10
}
