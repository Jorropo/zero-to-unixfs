package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
)

var cidSize int // precomputed size

func init() {
	mh, err := mh.Encode(make([]byte, 32), mh.SHA2_256)
	c(err)
	cidPrecomputed := cid.NewCidV1(uint64(multicodec.Raw), mh)
	cidSize = cidPrecomputed.ByteLen()

	cbor.RegisterCborType(CarHeader{})
}

type CarHeader struct {
	Roots   []cid.Cid
	Version uint64
}

const tempFileName = ".temp"

func main() {
	tempCar, err := os.OpenFile(tempFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0622)
	c(err)
	defer tempCar.Close()
	defer os.Remove(tempFileName)

	source, err := os.Open(os.Args[1])
	c(err)

	r := recurse{
		tempCar: tempCar,
	}

	root := r.add(source)

	outputCar, err := os.OpenFile(os.Args[2], os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0622)
	c(err)
	defer outputCar.Close()

	headerData, err := cbor.DumpObject(CarHeader{
		Roots:   []cid.Cid{root},
		Version: 1,
	})

	varintHeaderBuffer := make([]byte, binary.MaxVarintLen64)
	varuintSize := binary.PutUvarint(varintHeaderBuffer, uint64(len(headerData)))
	varintHeaderBuffer = varintHeaderBuffer[:varuintSize]

	_, err = outputCar.Write(varintHeaderBuffer)
	c(err)

	_, err = outputCar.Write(headerData)
	c(err)

	_, err = r.tempCar.Seek(0, io.SeekStart)
	c(err)

	_, err = outputCar.ReadFrom(tempCar)
	c(err)

	fmt.Println(root)
}

type recurse struct {
	tempCar *os.File
}

func (r recurse) add(source *os.File) cid.Cid {
	fileInfo, err := source.Stat()
	c(err)
	switch {
	case fileInfo.IsDir():
		panic("we do not support directories")
	case (fileInfo.Mode() & fs.ModeSymlink) != 0:
		panic("we do not support symlink")
	default:
		return r.addFile(source, fileInfo)
	}
}

func (r recurse) addFile(source *os.File, stat fs.FileInfo) cid.Cid {
	fileSize := uint64(stat.Size())

	// Element is CID + block
	elementSize := fileSize + uint64(cidSize)

	varuintEmptyCidBuffer := make([]byte, binary.MaxVarintLen64+cidSize)
	varuintSize := binary.PutUvarint(varuintEmptyCidBuffer, elementSize)
	varuintEmptyCidBuffer = varuintEmptyCidBuffer[:varuintSize+cidSize]

	_, err := r.tempCar.Write(varuintEmptyCidBuffer)
	c(err)

	blockHasher := sha256.New()
	_, err = io.Copy(r.tempCar, io.TeeReader(source, blockHasher))
	c(err)

	fileMh, err := mh.Encode(blockHasher.Sum(nil), mh.SHA2_256)
	c(err)
	blockCid := cid.NewCidV1(uint64(multicodec.Raw), fileMh)

	offset, err := r.tempCar.Seek(0, io.SeekCurrent)
	c(err)

	_, err = r.tempCar.WriteAt(blockCid.Bytes(), offset-int64(elementSize))
	c(err)

	return blockCid
}

func c(e error) {
	if e != nil {
		panic(e)
	}
}
