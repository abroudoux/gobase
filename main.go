package main

import (
	"fmt"
	"os"

	"gobase/buffer_pool_manager"
	"gobase/catalog"
	"gobase/disk_manager"
	"gobase/shared"
	"gobase/slotted_page"
	"gobase/table"
	"gobase/table_heap"
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

	fmt.Println("\n=== TEST CATALOG (Schema + Encoder/Decoder) ===")
	testCatalog()

	fmt.Println("\n=== TEST TABLE ===")
	testTable()

	fmt.Println("\n=== TEST LINKED PAGES ===")
	testLinkedPages()

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
	slotted_page.InitSlottedPage(page.Data)

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


func testCatalog() {
	// 1. Créer un schema
	schema := catalog.NewSchema([]catalog.Column{
		{Name: "id", Type: catalog.TypeInt},
		{Name: "name", Type: catalog.TypeVarchar, Size: 50},
		{Name: "age", Type: catalog.TypeSmallInt},
		{Name: "active", Type: catalog.TypeBoolean},
	})
	fmt.Println("1. Schema créé avec 4 colonnes: id(INT), name(VARCHAR), age(SMALLINT), active(BOOL)")

	// 2. Tester GetColumnIndex
	idx, err := schema.GetColumnIndex("age")
	if err != nil {
		fmt.Printf("ERREUR GetColumnIndex: %v\n", err)
		return
	}
	fmt.Printf("2. Index de la colonne 'age': %d\n", idx)

	// 3. Encoder un tuple
	values := []any{1, "Alice", 30, true}
	encoded := catalog.EncodeTuple(schema, values)
	fmt.Printf("3. Tuple encodé: %v (%d bytes)\n", encoded, len(encoded))

	// 4. Décoder le tuple
	decoded := catalog.DecodeTuple(schema, encoded)
	fmt.Printf("4. Tuple décodé: id=%v, name=%v, age=%v, active=%v\n",
		decoded[0], decoded[1], decoded[2], decoded[3])

	// 5. Vérifier que les valeurs sont identiques
	if decoded[0] == 1 && decoded[1] == "Alice" && decoded[2] == 30 && decoded[3] == true {
		fmt.Println("5. Encode/Decode OK: valeurs identiques!")
	} else {
		fmt.Println("5. ERREUR: valeurs différentes après decode")
	}
}

func testTable() {
	os.Remove("test.db")

	// 1. Setup: DiskManager + BufferPoolManager + TableHeap
	dm, err := disk_manager.NewDiskManager("test.db")
	if err != nil {
		fmt.Printf("ERREUR DiskManager: %v\n", err)
		return
	}
	bpm := buffer_pool_manager.NewBufferPoolManager(dm, 5)

	heap, err := table_heap.NewTableHeap(bpm)
	if err != nil {
		fmt.Printf("ERREUR TableHeap: %v\n", err)
		return
	}
	fmt.Println("1. Infrastructure créée (DiskManager + BufferPool + TableHeap)")

	// 2. Créer un schema
	schema := catalog.NewSchema([]catalog.Column{
		{Name: "id", Type: catalog.TypeInt},
		{Name: "name", Type: catalog.TypeVarchar, Size: 50},
		{Name: "age", Type: catalog.TypeSmallInt},
	})

	// 3. Créer la table
	usersTable := table.NewTable("users", schema, heap)
	fmt.Println("2. Table 'users' créée avec schema (id, name, age)")

	// 4. Insérer des données
	rid1, err := usersTable.Insert(1, "Alice", 30)
	if err != nil {
		fmt.Printf("ERREUR Insert: %v\n", err)
		return
	}
	fmt.Printf("3. Inséré: (1, 'Alice', 30) → RID(%d, %d)\n", rid1.GetPageID(), rid1.GetSlotID())

	rid2, err := usersTable.Insert(2, "Bob", 25)
	if err != nil {
		fmt.Printf("ERREUR Insert: %v\n", err)
		return
	}
	fmt.Printf("4. Inséré: (2, 'Bob', 25) → RID(%d, %d)\n", rid2.GetPageID(), rid2.GetSlotID())

	rid3, err := usersTable.Insert(3, "Charlie", 35)
	if err != nil {
		fmt.Printf("ERREUR Insert: %v\n", err)
		return
	}
	fmt.Printf("5. Inséré: (3, 'Charlie', 35) → RID(%d, %d)\n", rid3.GetPageID(), rid3.GetSlotID())

	// 5. Lire par RID
	row, err := usersTable.GetByRID(*rid2)
	if err != nil {
		fmt.Printf("ERREUR GetByRID: %v\n", err)
		return
	}
	fmt.Printf("6. GetByRID(rid2): id=%v, name=%v, age=%v\n", row[0], row[1], row[2])

	// 6. Scanner toute la table
	fmt.Println("7. Scan de la table:")
	scanner := usersTable.Scan()
	count := 0
	for {
		values, ok := scanner.Next()
		if !ok {
			break
		}
		count++
		fmt.Printf("   - Row %d: id=%v, name=%v, age=%v\n", count, values[0], values[1], values[2])
	}

	// 7. Supprimer une ligne
	err = usersTable.Delete(*rid2)
	if err != nil {
		fmt.Printf("ERREUR Delete: %v\n", err)
		return
	}
	fmt.Println("8. Supprimé: rid2 (Bob)")

	// 8. Re-scanner pour vérifier la suppression
	fmt.Println("9. Scan après suppression:")
	scanner2 := usersTable.Scan()
	count = 0
	for {
		values, ok := scanner2.Next()
		if !ok {
			break
		}
		count++
		fmt.Printf("   - Row %d: id=%v, name=%v, age=%v\n", count, values[0], values[1], values[2])
	}

	dm.Close()
	fmt.Println("10. Test Table terminé!")
}

func testLinkedPages() {
	os.Remove("test.db")

	dm, err := disk_manager.NewDiskManager("test.db")
	if err != nil {
		fmt.Printf("ERREUR DiskManager: %v\n", err)
		return
	}
	bpm := buffer_pool_manager.NewBufferPoolManager(dm, 3)

	heap, err := table_heap.NewTableHeap(bpm)
	if err != nil {
		fmt.Printf("ERREUR TableHeap: %v\n", err)
		return
	}

	schema := catalog.NewSchema([]catalog.Column{
		{Name: "id", Type: catalog.TypeInt},
		{Name: "data", Type: catalog.TypeVarchar, Size: 200},
	})

	t := table.NewTable("test_linked", schema, heap)
	fmt.Println("1. Table créée avec schema (id INT, data VARCHAR(200))")

	// Insérer suffisamment de tuples pour remplir plusieurs pages
	// Chaque tuple fait environ 200+ bytes, une page de 4096 bytes peut en contenir ~15-20
	numRows := 100
	fmt.Printf("2. Insertion de %d lignes pour forcer plusieurs pages...\n", numRows)

	for i := 0; i < numRows; i++ {
		data := fmt.Sprintf("Row-%d-padding-to-make-this-tuple-larger-%s", i, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
		_, err := t.Insert(i, data)
		if err != nil {
			fmt.Printf("ERREUR Insert row %d: %v\n", i, err)
			return
		}
	}
	fmt.Printf("3. %d lignes insérées avec succès\n", numRows)

	// Scanner et compter
	scanner := t.Scan()
	count := 0
	for {
		_, ok := scanner.Next()
		if !ok {
			break
		}
		count++
	}

	fmt.Printf("4. Scan terminé: %d lignes lues\n", count)

	if count == numRows {
		fmt.Println("5. SUCCESS: Les pages chaînées fonctionnent correctement!")
	} else {
		fmt.Printf("5. ERREUR: Attendu %d lignes, obtenu %d\n", numRows, count)
	}

	dm.Close()
}
