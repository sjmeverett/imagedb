package main

import (
  "fmt"
  "github.com/spf13/cobra"
  "github.com/boltdb/bolt"
  "log"
  "os"
)


func runInit(cmd *cobra.Command, args []string) {
  if len(args) < 1 {
    log.Fatal("no file specified")
  }

  filename := args[0]

  if _, err := os.Stat(filename); err == nil {
    log.Fatal("file already exists")
  }

  db, err := bolt.Open(filename, 0600, nil)

  if err != nil {
    log.Fatal(err)
  }

  defer db.Close()

  db.Update(func(tx *bolt.Tx) error {
    system, err := tx.CreateBucket([]byte("sys"))

    system.Put([]byte(""))

    return err
  })
}



func main() {
  initCmd := &cobra.Command{
    Use: "init [dbfile]",
    Short: "initialise a new database file at the specified path",
    Run: runInit,
  }

  rootCmd := &cobra.Command{Use: "idb"}
  rootCmd.AddCommand(initCmd)
  rootCmd.Execute()
}
