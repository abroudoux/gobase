# Documentation technique — gobase

Implémentation d'une base de données relationnelle en Go, couche par couche.

## Couches (du plus bas niveau au plus haut)

| Fichier | Couche | Rôle |
|---------|--------|------|
| [01_disk_manager.md](./01_disk_manager.md) | Disk Manager | Lecture/écriture de pages sur le disque |
| [02_buffer_pool_manager.md](./02_buffer_pool_manager.md) | Buffer Pool Manager | Cache de pages en mémoire |
| [03_slotted_page.md](./03_slotted_page.md) | Slotted Page | Stockage de tuples de taille variable dans une page |
| [04_catalog.md](./04_catalog.md) | Catalog | Schéma et sérialisation binaire des tuples |
| [05_table_heap.md](./05_table_heap.md) | Table Heap | Gestion d'une table sur plusieurs pages |
| [06_table.md](./06_table.md) | Table | API haut niveau avec types Go |
| [07_bplus_tree_index.md](./07_bplus_tree_index.md) | B+ Tree Index | Index pour recherches en O(log n) |

## Flux de données — Insertion d'une ligne

```
table.Insert(1, "Alice", 30)
    │
    ▼
catalog.EncodeTuple()        → [01 00 00 00 05 00 Alice 1E 00]
    │
    ▼
table_heap.Insert(bytes)     → cherche une page avec de la place
    │
    ▼
buffer_pool_manager.FetchPage() → charge la page en RAM si besoin
    │
    ▼
slotted_page.InsertTuple()   → écrit dans la page, crée un slot
    │
    ▼
buffer_pool_manager.UnpinPage(dirty=true)
    │
    ▼ (au flush)
disk_manager.WritePage()     → persiste sur le disque
```
