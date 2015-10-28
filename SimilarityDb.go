
package similaritydb

import (
  "bytes"
  "github.com/boltdb/bolt"
  "encoding/binary"
  "math"
)

type SimilarityDb struct {
  db *bolt.DB
  bucket []byte
}


type VPNode struct {
  ID int
  Left int
  Right int
  Key [80]float64
  Radius float64
  normalisedKey [80]float64
  l1Norm float64
  complexity float64
  distanceToParent float64
}


func Create(db *bolt.DB, bucket string) SimilarityDb {
  return SimilarityDb{db, []byte(bucket)}
}


func (db *SimilarityDb) Insert(key [80]float64) (int, error) {
  node := createNode(key)

  err := db.db.Update(func(tx *bolt.Tx) error {
    bucket := tx.Bucket(db.bucket)
    id, _ := bucket.NextSequence()
    node.ID = int(id)

    root, err := get(bucket, 0)
    if err != nil {
      return err
    }

    return insert(bucket, root, node)
  })

  if err != nil {
    return -1, err
  }

  return node.ID, nil
}


func createNode(key [80]float64) *VPNode {
  var normalisedKey [80]float64
  total := 0.0

  // calculate the normalised key
  for _, d := range key {
    total += d
  }

  if total > 0 {
    for i, d := range key {
      normalisedKey[i] = d / total
    }
  }

  // calculate the complexity
  acc := 0.0

  for _, d := range normalisedKey {
    if d != 0 {
      acc -= d * math.Log(d)
    }
  }

  complexity := math.Exp(acc)

  return &VPNode{
    ID: -1,
    Left: -1,
    Right: -1,
    Key: key,
    l1Norm: total,
    normalisedKey: normalisedKey,
    complexity: complexity,
  }
}


func insert(bucket *bolt.Bucket, parent *VPNode, node *VPNode) error {
  distance := getDistance(parent, node)

  if node.Left  == -1 {
    parent.Left = node.ID
    parent.Radius = distance
    return create(bucket, parent, node, distance)

  } else if distance <= parent.Radius {
    p, err := get(bucket, parent.Left)
    if err != nil {
      return err
    }

    return insert(bucket, p, node)

  } else if parent.Right == -1 {
    parent.Right = node.ID
    return create(bucket, parent, node, distance)

  } else {
    p, err := get(bucket, parent.Right)
    if err != nil {
      return err
    }

    return insert(bucket, p, node)
  }
}


func create(bucket *bolt.Bucket, parent *VPNode, node *VPNode, distance float64) error {
  err := put(bucket, parent.ID, parent)
  if err != nil {
    return err
  }

  node.distanceToParent = distance
  return put(bucket, node.ID, node)
}


func get(bucket *bolt.Bucket, id int) (*VPNode, error) {
  buffer := bucket.Get(intToBytes(id))

  reader := bytes.NewReader(buffer)
  var node *VPNode

  err := binary.Read(reader, binary.LittleEndian, node)
  if err != nil {
    return nil, err
  }

  return node, nil
}


func put(bucket *bolt.Bucket, id int, node *VPNode) error {
  buffer := new(bytes.Buffer)

  err := binary.Write(buffer, binary.LittleEndian, node)
  if  err != nil {
    return err
  }

  return bucket.Put(intToBytes(id), buffer.Bytes())
}


func (db *SimilarityDb) Close() {
  db.db.Close()
}




func intToBytes(v int) []byte {
  b := make([]byte, 8)
  binary.BigEndian.PutUint64(b, uint64(v))
  return b
}
