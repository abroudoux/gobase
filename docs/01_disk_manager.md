# Disk Manager

## Rôle

Le Disk Manager est la couche la plus basse de la base de données. Son unique responsabilité est de lire et d'écrire des **pages** sur le disque. Il ne connaît rien des tuples, des index ou des tables — il manipule uniquement des blocs d'octets de taille fixe.

## Concept de page

Une base de données ne lit pas les fichiers octet par octet. Elle découpe son fichier en **pages** de taille fixe (ici 4096 octets, soit 4KB — la taille standard d'une page OS).

```go
const PAGE_SIZE uint32 = 4096
```

Chaque page est identifiée par un `pageID` (entier), et son emplacement sur le disque se calcule simplement :

```
offset = pageID * PAGE_SIZE
```

Ainsi, la page 0 est à l'octet 0, la page 1 à l'octet 4096, la page 2 à l'octet 8192, etc.

## Structure

```go
type DiskManager struct {
    File     *os.File
    PageSize uint32
    NumPages uint32
}
```

- `File` : le fichier de base de données ouvert
- `PageSize` : toujours 4096
- `NumPages` : nombre de pages actuellement allouées (permet de savoir où allouer la prochaine page)

## Opérations

### Lire une page

```go
func (dm *DiskManager) ReadPage(pageID uint32) ([]byte, error)
```

1. Calcule l'offset : `pageID * PAGE_SIZE`
2. Se positionne dans le fichier avec `Seek`
3. Lit exactement `PAGE_SIZE` octets

Si le fichier fait moins d'octets que l'offset demandé, la page n'existe pas.

### Écrire une page

```go
func (dm *DiskManager) WritePage(pageID uint32, data []byte) error
```

1. Calcule l'offset
2. Se positionne avec `Seek`
3. Écrit les données
4. Appelle `Sync()` pour forcer l'écriture sur le disque physique (durabilité)

### Allouer une page

```go
func (dm *DiskManager) AllocatePage() (uint32, error)
```

Crée une nouvelle page vide à la fin du fichier :

1. `newPageID = dm.NumPages`
2. Écrit une page de zéros à cet emplacement
3. Incrémente `NumPages`
4. Retourne le nouvel ID

## Pourquoi des pages de taille fixe ?

- **Accès direct** : calculer un offset est O(1), pas besoin de parcourir le fichier
- **Gestion mémoire simplifiée** : on alloue toujours des blocs de même taille
- **Alignement OS** : correspond à la taille de page du système de fichiers, ce qui optimise les I/O

## Schéma

```
Fichier sur disque :
+----------+----------+----------+----------+
|  Page 0  |  Page 1  |  Page 2  |  Page 3  |
| 4096 B   | 4096 B   | 4096 B   | 4096 B   |
+----------+----------+----------+----------+
offset:  0       4096      8192     12288
```
