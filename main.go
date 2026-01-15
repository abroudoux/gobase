package main

import "fmt"

func main() {
	dm1, err := NewDiskManager("test.db")
	if err != nil {
		fmt.Printf("%v", err)
	}

	pageID, err := dm1.AllocatePage()
	if err != nil {
		fmt.Printf("%v", err)
	}

	data := make([]byte, PAGE_SIZE)
	data[0] = 'H'
	data[1] = 'e'
	data[2] = 'l'
	data[3] = 'l'
	data[4] = 'o'
	err = dm1.WritePage(pageID, data)
	if err != nil {
		fmt.Printf("%v", err)
	}

	dm1.Close()

	dm2, err := NewDiskManager("test.db")
	if err != nil {
		fmt.Printf("%v", err)
	}

	content, err := dm2.ReadPage(0)
	if err != nil {
		fmt.Printf("%v", err)
	}

	fmt.Printf("Content: %s\n", string(content[:len(data)]))
	dm2.Close()
}
