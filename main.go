package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"

	pb "github.com/Jorropo/zero-to-unixfs/pb"
	proto "google.golang.org/protobuf/proto"

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
const blockSize = 1024 * 1024 // 1MiB
const fanOutSize = 5

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

func (r recurse) addFile(source *os.File, fileInfo fs.FileInfo) cid.Cid {
	fileSize := fileInfo.Size()

	var leavesBlock uint64
	if fileSize == 0 {
		leavesBlock = 1
	} else {
		// Div round up
		// fileSize = 10
		// blockSize = 5
		// Let's assume we do 9 / 5
		// 9 - 1 = 8
		// 8 / 5 = 1
		// 1 + 1 = 2 = roundup(10 / 5)
		leavesBlock = (uint64(fileSize)-1)/blockSize + 1
	}

	leaves := make([]cidSizePair, leavesBlock)
	for i := range leaves {
		leafSize := uint64(blockSize)
		if uint64(i) == leavesBlock-1 { // last block
			leafSize = uint64(fileSize) % blockSize
		}
		leaves[i] = r.addBlock(source, leafSize)
	}

	roots := leaves
	for len(roots) > 1 {
		leaves = roots
		roots = nil
		for len(leaves) > 1 {

			// avoid links to nothing if there is less leaves than the fanOutSize
			linkCount := fanOutSize
			if len(leaves) < linkCount {
				linkCount = len(leaves)
			}

			blocksizes := make([]uint64, linkCount)
			links := make([]*pb.PBLink, linkCount)
			var fileSize uint64
			var dagSize uint64

			for i, v := range leaves[:linkCount] {
				fileSize += v.fileSize
				dagSize += v.dagSize
				links[i] = &pb.PBLink{
					Hash:  v.c.Bytes(),
					Tsize: &v.dagSize,
				}
				blocksizes[i] = v.fileSize
			}

			t := pb.UnixfsData_File
			rootData := &pb.UnixfsData{
				Type:       &t,
				Filesize:   &fileSize,
				Blocksizes: blocksizes,
			}
			unixfsData, err := proto.Marshal(rootData)
			c(err)

			root := &pb.PBNode{
				Data:  unixfsData,
				Links: links,
			}
			rootBuffer, err := proto.Marshal(root)
			c(err)
			hasher := sha256.New()
			hasher.Write(rootBuffer)

			rootMultihash, err := mh.Encode(hasher.Sum(nil), mh.SHA2_256)
			c(err)

			rootCid := cid.NewCidV1(cid.DagProtobuf, rootMultihash)

			elementSize := uint64(len(rootBuffer)) + uint64(rootCid.ByteLen())

			varuintRootCid := make([]byte, binary.MaxVarintLen64)
			varuintSize := binary.PutUvarint(varuintRootCid, elementSize)
			varuintRootCid = varuintRootCid[:varuintSize]

			_, err = r.tempCar.Write(varuintRootCid)
			c(err)
			_, err = r.tempCar.Write(rootCid.Bytes())
			c(err)
			_, err = r.tempCar.Write(rootBuffer)
			c(err)

			dagSize += uint64(len(rootBuffer))

			roots = append(roots, cidSizePair{
				c:        rootCid,
				fileSize: fileSize,
				dagSize:  dagSize,
			})
			leaves = leaves[linkCount:]
		}
		if len(leaves) == 1 {
			roots = append(roots, leaves[0])
		}
	}
	return roots[0].c
}

type cidSizePair struct {
	c        cid.Cid
	fileSize uint64
	// dag size is the size of all the blocks
	// containing protobuf and linking overhead
	dagSize uint64
}

func (r recurse) addBlock(source *os.File, blockSize uint64) cidSizePair {
	// Element is CID + block
	elementSize := blockSize + uint64(cidSize)

	varuintEmptyCidBuffer := make([]byte, binary.MaxVarintLen64+cidSize)
	varuintSize := binary.PutUvarint(varuintEmptyCidBuffer, elementSize)
	varuintEmptyCidBuffer = varuintEmptyCidBuffer[:varuintSize+cidSize]

	_, err := r.tempCar.Write(varuintEmptyCidBuffer)
	c(err)

	blockHasher := sha256.New()
	_, err = io.CopyN(r.tempCar, io.TeeReader(source, blockHasher), int64(blockSize))
	c(err)

	fileMh, err := mh.Encode(blockHasher.Sum(nil), mh.SHA2_256)
	c(err)
	blockCid := cid.NewCidV1(uint64(multicodec.Raw), fileMh)

	offset, err := r.tempCar.Seek(0, io.SeekCurrent)
	c(err)

	_, err = r.tempCar.WriteAt(blockCid.Bytes(), offset-int64(elementSize))
	c(err)

	return cidSizePair{blockCid, blockSize, blockSize}
}

func c(e error) {
	if e != nil {
		panic(e)
	}
}
