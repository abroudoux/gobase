package main

import (
	"fmt"
	"os"

	"gobase/buffer_pool_manager"
	"gobase/disk_manager"
)

func main() {
	// Supprimer le fichier de test s'il existe
	os.Remove("test.db")

	fmt.Println("=== TEST DISK MANAGER ===")
	testDiskManager()

	fmt.Println("\n=== TEST BUFFER POOL MANAGER ===")
	testBufferPoolManager()

	fmt.Println("\n=== TEST EVICTION DIRTY ===")
	testEvictionDirty()

	// Nettoyage
	os.Remove("test.db")
	fmt.Println("\nTous les tests sont terminés!")
}

func testDiskManager() {
	// 1. Créer un DiskManager
	dm, err := disk_manager.NewDiskManager("test.db")
	if err != nil {
		fmt.Printf("ERREUR création DiskManager: %v\n", err)
		return
	}
	fmt.Println("1. DiskManager créé")

	// 2. Allouer une page
	pageID, err := dm.AllocatePage()
	if err != nil {
		fmt.Printf("ERREUR allocation: %v\n", err)
		return
	}
	fmt.Printf("2. Page allouée avec ID: %d\n", pageID)

	// 3. Écrire des données
	data := make([]byte, disk_manager.PAGE_SIZE)
	copy(data, []byte("Hello DiskManager!"))
	err = dm.WritePage(pageID, data)
	if err != nil {
		fmt.Printf("ERREUR écriture: %v\n", err)
		return
	}
	fmt.Println("3. Données écrites sur la page")

	// 4. Fermer le DiskManager
	dm.Close()
	fmt.Println("4. DiskManager fermé")

	// 5. Rouvrir et vérifier la persistance
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
	// Nettoyer
	os.Remove("test.db")

	// 1. Créer DiskManager et BufferPoolManager
	dm, _ := disk_manager.NewDiskManager("test.db")
	bpm := buffer_pool_manager.NewBufferPoolManager(dm, 3) // Pool de 3 pages
	fmt.Println("1. BufferPoolManager créé avec poolSize=3")

	// 2. Créer une nouvelle page via BufferPoolManager
	pageID1, page1, err := bpm.NewPage()
	if err != nil {
		fmt.Printf("ERREUR NewPage: %v\n", err)
		return
	}
	copy(page1.Data, []byte("Page 1 - Bonjour!"))
	fmt.Printf("2. Nouvelle page créée avec ID: %d\n", pageID1)

	// 3. Créer une deuxième page
	pageID2, page2, err := bpm.NewPage()
	if err != nil {
		fmt.Printf("ERREUR NewPage: %v\n", err)
		return
	}
	copy(page2.Data, []byte("Page 2 - Hello!"))
	fmt.Printf("3. Deuxième page créée avec ID: %d\n", pageID2)

	// 4. Unpin les pages (marquer page1 comme dirty)
	bpm.UnpinPage(pageID1, true)  // dirty = true
	bpm.UnpinPage(pageID2, false) // dirty = false
	fmt.Println("4. Pages unpinned (page1 dirty, page2 clean)")

	// 5. Fetch la page 1 (devrait être en cache)
	fetchedPage, err := bpm.FetchPage(pageID1)
	if err != nil {
		fmt.Printf("ERREUR FetchPage: %v\n", err)
		return
	}
	fmt.Printf("5. Page 1 récupérée du cache: %s\n", string(fetchedPage.Data[:17]))
	bpm.UnpinPage(pageID1, false)

	// 6. Flush la page 1 sur disque
	err = bpm.FlushPage(pageID1)
	if err != nil {
		fmt.Printf("ERREUR FlushPage: %v\n", err)
		return
	}
	fmt.Println("6. Page 1 flushée sur disque")

	// 7. Vérifier la persistance
	dm.Close()
	dm2, _ := disk_manager.NewDiskManager("test.db")
	readData, _ := dm2.ReadPage(pageID1)
	fmt.Printf("7. Vérification persistance: %s\n", string(readData[:17]))
	dm2.Close()
}

func testEvictionDirty() {
	// Nettoyer
	os.Remove("test.db")

	// Créer un pool de seulement 2 pages pour forcer l'éviction
	dm, _ := disk_manager.NewDiskManager("test.db")
	bpm := buffer_pool_manager.NewBufferPoolManager(dm, 2)
	fmt.Println("1. BufferPoolManager créé avec poolSize=2 (pour forcer éviction)")

	// Créer 2 pages (remplit le pool)
	pageID1, page1, _ := bpm.NewPage()
	copy(page1.Data, []byte("PAGE-1-ORIGINAL"))
	bpm.UnpinPage(pageID1, true) // dirty!
	fmt.Printf("2. Page %d créée et marquée dirty\n", pageID1)

	pageID2, page2, _ := bpm.NewPage()
	copy(page2.Data, []byte("PAGE-2-DATA"))
	bpm.UnpinPage(pageID2, false)
	fmt.Printf("3. Page %d créée (clean)\n", pageID2)

	// Le pool est plein. Créer une 3ème page force l'éviction
	fmt.Println("4. Pool plein, création d'une 3ème page (force éviction)...")

	pageID3, _, err := bpm.NewPage()
	if err != nil {
		fmt.Printf("ERREUR: %v\n", err)
		return
	}
	fmt.Printf("5. Page %d créée, une page a été évincée\n", pageID3)

	// Vérifier que la page 1 (dirty) a bien été écrite sur disque
	dm.Close()
	dm2, _ := disk_manager.NewDiskManager("test.db")
	readData, err := dm2.ReadPage(pageID1)
	if err != nil {
		fmt.Printf("ERREUR lecture page évincée: %v\n", err)
		return
	}
	fmt.Printf("6. Page 1 évincée et sauvegardée: %s\n", string(readData[:15]))
	dm2.Close()
}
