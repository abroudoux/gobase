package table_heap

import "gobase/shared"

func (it *TableIterator) Next() (*RID, shared.Tuple, bool) {
	// 1. Vérifier si on a dépassé la dernière page
	// 2. Fetch la page courante
	// 3. Essayer de lire le tuple au slot courant
	// 4. Si slot vide/supprimé → passer au suivant
	// 5. Si fin de page → passer à page suivante
	// 6. Retourner (rid, tuple, true) ou (_, _, false) si fin
	//
	//   Cas particuliers à gérer
	// 1. Slots supprimés : DeleteTuple met length=0, il faut les sauter
	// 2. Pages chaînées : Comment savoir quelle est la page suivante ?
	// 3. Fin de table : Quand currentPageID > lastPageID

	// Question importante

	// Actuellement, tes pages ne sont pas chaînées. Comment sait-on quelle est la page suivante ?

	// Options :
	// - Option A : Pages contiguës (pageID 0, 1, 2, 3...) → simple
	// - Option B : Chaîner les pages (chaque page pointe vers la suivante)

	// Pour commencer, Option A est plus simple : on suppose que les pages vont de firstPageID à lastPageID de manière contiguë.

	return nil, nil, false
}
