# Buffer Pool Manager

## Rôle

Lire et écrire sur le disque est lent (millisecondes) comparé à la RAM (nanosecondes). Le Buffer Pool Manager est un **cache de pages en mémoire** : il garde les pages fréquemment utilisées en RAM pour éviter d'aller sur le disque à chaque opération.

## Concepts clés

### Frame

Un **frame** est un emplacement mémoire pouvant contenir une page. Le buffer pool est un tableau de frames de taille fixe.

```go
type Frame struct {
    PageID   uint32
    Data     []byte
    Dirty    bool
    PinCount int
}
```

- `Dirty` : la page a été modifiée en mémoire mais pas encore écrite sur le disque
- `PinCount` : nombre de composants qui utilisent actuellement cette page. Une page avec `PinCount > 0` ne peut pas être évictée

### Page Table

Une map `pageID → index de frame` permet de savoir si une page est déjà en cache et dans quel frame elle se trouve.

```go
type BufferPoolManager struct {
    frames    []*Frame
    pageTable map[uint32]int
    dm        *disk_manager.DiskManager
    poolSize  int
}
```

## Opérations

### FetchPage

```go
func (bpm *BufferPoolManager) FetchPage(pageID uint32) (*Frame, error)
```

1. **Cache hit** : si `pageID` est dans `pageTable`, retourne le frame directement et incrémente `PinCount`
2. **Cache miss** : trouve un frame libre, charge la page depuis le disque, l'ajoute à `pageTable`

### UnpinPage

```go
func (bpm *BufferPoolManager) UnpinPage(pageID uint32, isDirty bool) error
```

Signale qu'on n'utilise plus la page. Décrémente `PinCount`. Si `isDirty = true`, marque le frame comme dirty.

**Important** : toute page fetchée doit être unpinnée après usage, sinon le buffer pool se remplit et aucune éviction n'est possible.

### Éviction

Quand tous les frames sont occupés et qu'on a besoin d'un nouveau frame :

1. Cherche un frame avec `PinCount == 0` (non utilisé)
2. Si ce frame est `Dirty`, écrit la page sur le disque avant de la supprimer
3. Retire l'entrée de `pageTable`
4. Le frame est maintenant disponible

### FlushPage

```go
func (bpm *BufferPoolManager) FlushPage(pageID uint32) error
```

Force l'écriture d'une page dirty sur le disque sans l'évicterr du cache.

### NewPage

```go
func (bpm *BufferPoolManager) NewPage() (*Frame, error)
```

1. Alloue une nouvelle page sur le disque via le DiskManager
2. La charge dans un frame disponible
3. La retourne prête à l'emploi

## Cycle de vie d'une page

```
FetchPage(5)          → PinCount = 1, page chargée en mémoire
... modifications ...
UnpinPage(5, true)    → PinCount = 0, Dirty = true
... plus tard ...
FlushPage(5)          → Dirty = false, page écrite sur disque
... besoin d'espace ...
éviction de la page 5 → retirée du cache
FetchPage(5)          → chargée à nouveau depuis le disque
```

## Schéma

```
Buffer Pool (4 frames) :
+----------+----------+----------+----------+
| Frame 0  | Frame 1  | Frame 2  | Frame 3  |
| Page 3   | Page 7   | vide     | Page 1   |
| Pin=0    | Pin=2    |          | Pin=1    |
| Dirty=T  | Dirty=F  |          | Dirty=F  |
+----------+----------+----------+----------+

Page Table : { 3→0, 7→1, 1→3 }

Disque :
Page 1, Page 2, Page 3, Page 4, Page 5, Page 6, Page 7 ...
```

Si on demande Page 5 (cache miss) : Frame 0 est évictable (Pin=0). Sa page 3 est dirty → on l'écrit sur disque. On charge Page 5 dans Frame 0.
