//go:build windows

package gui

import (
	"bytes"
	_ "embed"
	"encoding/binary"
	"image"
	"image/png"
	"unsafe"

	"golang.org/x/sys/windows"
)

//go:embed icon.png
var iconPNG []byte

const wmSetIcon = 0x0080

// setWindowIcon sets the MASS icon for the taskbar (ICON_BIG) and a
// blank transparent icon for the title bar (ICON_SMALL) to hide it.
func setWindowIcon(hwnd unsafe.Pointer) {
	bigIcon := createIconFromPNG(iconPNG, 256)
	if bigIcon == 0 {
		return
	}
	blankIcon := createBlankIcon()

	sendMessage := user32.NewProc("SendMessageW")
	h := uintptr(hwnd)
	if blankIcon != 0 {
		sendMessage.Call(h, wmSetIcon, 0, blankIcon) //nolint:errcheck // syscall return value not meaningful
	}
	sendMessage.Call(h, wmSetIcon, 1, bigIcon) //nolint:errcheck // syscall return value not meaningful
}

// createBlankIcon creates a 1x1 fully transparent icon.
func createBlankIcon() uintptr {
	gdi32 := windows.NewLazySystemDLL("gdi32.dll")
	createBitmap := gdi32.NewProc("CreateBitmap")
	deleteObject := gdi32.NewProc("DeleteObject")
	createIconIndirect := user32.NewProc("CreateIconIndirect")

	andBits := [4]byte{0xFF, 0, 0, 0}
	xorBits := [4]byte{0, 0, 0, 0}

	hbmMask, _, _ := createBitmap.Call(1, 1, 1, 1, uintptr(unsafe.Pointer(&andBits[0])))
	hbmColor, _, _ := createBitmap.Call(1, 1, 1, 1, uintptr(unsafe.Pointer(&xorBits[0])))
	if hbmMask == 0 || hbmColor == 0 {
		return 0
	}
	defer func() {
		deleteObject.Call(hbmMask)  //nolint:errcheck // syscall
		deleteObject.Call(hbmColor) //nolint:errcheck // syscall
	}()

	type blankIconInfo struct {
		FIcon    int32
		XHotspot int32
		YHotspot int32
		HbmMask  uintptr
		HbmColor uintptr
	}
	ii := blankIconInfo{FIcon: 1, HbmMask: hbmMask, HbmColor: hbmColor}
	icon, _, _ := createIconIndirect.Call(uintptr(unsafe.Pointer(&ii)))
	return icon
}

// createIconFromPNG decodes a PNG, scales it to size×size, and returns an HICON.
func createIconFromPNG(data []byte, size int) uintptr {
	src, err := png.Decode(bytes.NewReader(data))
	if err != nil {
		return 0
	}

	// Draw into a square NRGBA canvas (centered).
	dst := image.NewNRGBA(image.Rect(0, 0, size, size))
	srcB := src.Bounds()

	// Scale to fit within size×size, maintaining aspect ratio.
	srcW, srcH := srcB.Dx(), srcB.Dy()
	scale := float64(size) / float64(srcW)
	if s := float64(size) / float64(srcH); s < scale {
		scale = s
	}
	dstW := int(float64(srcW) * scale)
	dstH := int(float64(srcH) * scale)
	offX := (size - dstW) / 2
	offY := (size - dstH) / 2

	// Simple nearest-neighbor resize + center.
	for y := 0; y < dstH; y++ {
		srcY := srcB.Min.Y + y*srcH/dstH
		for x := 0; x < dstW; x++ {
			srcX := srcB.Min.X + x*srcW/dstW
			dst.Set(offX+x, offY+y, src.At(srcX, srcY))
		}
	}

	return nrgbaToHICON(dst)
}

// nrgbaToHICON converts an NRGBA image to a Windows HICON.
func nrgbaToHICON(img *image.NRGBA) uintptr {
	w := img.Bounds().Dx()
	h := img.Bounds().Dy()

	// Build BGRA pixel data (bottom-up for DIB).
	pixels := make([]byte, w*h*4)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			si := y*img.Stride + x*4
			di := (h-1-y)*w*4 + x*4
			pixels[di+0] = img.Pix[si+2] // B
			pixels[di+1] = img.Pix[si+1] // G
			pixels[di+2] = img.Pix[si+0] // R
			pixels[di+3] = img.Pix[si+3] // A
		}
	}

	gdi32 := windows.NewLazySystemDLL("gdi32.dll")
	createBitmap := gdi32.NewProc("CreateBitmap")
	createDIBSection := gdi32.NewProc("CreateDIBSection")
	deleteObject := gdi32.NewProc("DeleteObject")
	createIconIndirect := user32.NewProc("CreateIconIndirect")

	// BITMAPINFOHEADER for the color bitmap.
	var bih [40]byte
	binary.LittleEndian.PutUint32(bih[0:], 40)          // biSize
	binary.LittleEndian.PutUint32(bih[4:], uint32(w))   // biWidth
	binary.LittleEndian.PutUint32(bih[8:], uint32(h*2)) // biHeight (×2 for AND+XOR in icon)
	binary.LittleEndian.PutUint16(bih[12:], 1)          // biPlanes
	binary.LittleEndian.PutUint16(bih[14:], 32)         // biBitCount
	// biCompression = BI_RGB = 0 (default)

	// For CreateDIBSection we need height = h (not h*2).
	var bihDIB [40]byte
	copy(bihDIB[:], bih[:])
	binary.LittleEndian.PutUint32(bihDIB[8:], uint32(h))

	var ppvBits uintptr
	hbmColor, _, _ := createDIBSection.Call(0, uintptr(unsafe.Pointer(&bihDIB[0])), 0, uintptr(unsafe.Pointer(&ppvBits)), 0, 0)
	if hbmColor == 0 || ppvBits == 0 {
		return 0
	}

	// Copy pixel data into the DIB section.
	copy(unsafe.Slice((*byte)(unsafe.Pointer(ppvBits)), len(pixels)), pixels) //nolint:govet // ppvBits is a valid pointer from CreateDIBSection

	// Monochrome AND mask (all zeros = opaque; alpha channel in color bitmap handles transparency).
	maskSize := ((w + 31) / 32) * 4 * h
	andBits := make([]byte, maskSize) // all zeros
	hbmMask, _, _ := createBitmap.Call(uintptr(w), uintptr(h), 1, 1, uintptr(unsafe.Pointer(&andBits[0])))
	if hbmMask == 0 {
		deleteObject.Call(hbmColor) //nolint:errcheck // syscall
		return 0
	}

	type iconInfo struct {
		FIcon    int32
		XHotspot int32
		YHotspot int32
		HbmMask  uintptr
		HbmColor uintptr
	}

	ii := iconInfo{
		FIcon:    1,
		HbmMask:  hbmMask,
		HbmColor: hbmColor,
	}
	icon, _, _ := createIconIndirect.Call(uintptr(unsafe.Pointer(&ii)))

	deleteObject.Call(hbmMask)  //nolint:errcheck // syscall
	deleteObject.Call(hbmColor) //nolint:errcheck // syscall

	return icon
}
