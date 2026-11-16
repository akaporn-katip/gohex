# วิธี release version ใหม่

ฉบับภาษาไทยของ `RELEASING.md` — เนื้อหาเดียวกัน ถ้าขัดกันให้ยึดฉบับภาษาอังกฤษ

gohex เป็น **Go module เดียว** ที่ tag ไว้ที่ root (`vX.Y.Z`) ทุก package version
ไปพร้อมกัน และผู้ใช้ขึ้นต่อ module รวม (`github.com/akaporn-katip/gohex`)
ไม่ใช่ subpackage แยกกัน

## ประวัติ: ทำไมเลิกแยก Module

release แรก ๆ ใช้ multi-module layout: 12 module แยกกัน tag ทีละตัว
(`eventstore/v0.3.0`, `o11y/v0.5.1` ฯลฯ) แนวคิดดูดี — version แยกอิสระ —
แต่ต้นทุนการดูแลสูงมาก: ทุก release ต้อง pin module พี่น้องตามลำดับ dependency
push tag ทีละตัว และเดินวน hand-off ระหว่างคนเตรียมกับคนมีสิทธิ์ push

แผลเป็นที่ทำให้เกิดกฎ **public before pinned** มาจาก v0.3.0/v0.4.0/v0.5.1
ที่ release ด้วย go.sum คำนวณจาก archive ที่สร้าง local เอง พอ tag ขึ้น public
sum.golang.org hash ได้คนละค่า ทำให้ build พัง checksum ซ่อมด้วย repair release
(v0.3.1/v0.4.1/v0.5.2)

module รวมตัดปัญหาทั้งหมด: tag เดียว, `go mod tidy` ครั้งเดียว, ไม่มี sibling
dependency ต้องจัดการ, ไม่มี dependency level ต้องเดิน

## ขั้นตอน

### 1. พัฒนาและทดสอบ

เงื่อนไขผ่าน:

```sh
task test                                # เขียวหมด
GOWORK=off go vet ./...                  # สะอาด
gofmt -l . | grep -v gohex-example       # ไม่มี output
```

### 2. ตัดสิน version

ใช้ semver แบบ v0.x:

- เพิ่ม method ลง interface สาธารณะ → **minor bump**
- เปลี่ยน signature, parameter type, behavior guarantee → **major bump** (จะเป็น v1.0.0)

### 3. Commit, tag, และ push

```sh
git commit -am "release: vX.Y.Z — <สิ่งที่เปลี่ยน>"
git tag vX.Y.Z
git push origin main --tags
```

### 4. ตรวจสอบ

consumer build สะอาดพิสูจน์ว่า release ถูกต้อง:

```sh
mkdir /tmp/consumer-test && cd /tmp/consumer-test
go mod init example.com/m
GOPROXY=https://proxy.golang.org go get github.com/akaporn-katip/gohex@vX.Y.Z
go list -m github.com/akaporn-katip/gohex
```

## การย้ายจาก Multi-Module Tags

ผู้ใช้ที่อัพเกรดจาก release แบบ multi-module (tag อย่าง `eventstore/v0.3.0`)
มาใช้ module รวม **ต้องลบ `require` ของ subpackage ทั้งหมดในคราวเดียว**

Go module resolution ไม่รองรับ ambiguous import: ถ้า `go.mod` มีทั้ง
`require github.com/akaporn-katip/gohex vX.Y.Z` และ
`require github.com/akaporn-katip/gohex/eventstore v0.3.0` พร้อมกัน build พัง

วิธีย้าย:

1. ลบทุกบรรทัด `require github.com/akaporn-katip/gohex/<subpackage>`
2. เพิ่ม `require github.com/akaporn-katip/gohex vX.Y.Z`
3. รัน `go mod tidy`
4. **ไม่ต้องแก้ import statement** — `import "github.com/akaporn-katip/gohex/eventstore"`
   ยังใช้ได้เหมือนเดิม เพราะ prefix คือ module path (ตอนนี้คือ module รวม)
   ส่วนหลังคือ package path

Release รวมตัวแรกจะเป็น `v0.7.0` หรือสูงกว่า (ยังไม่กำหนด)

## Checklist ย่อ

- [ ] `task test` เขียว
- [ ] `GOWORK=off go vet ./...` สะอาด
- [ ] `gofmt -l .` (ยกเว้น `gohex-example/`) ไม่มี output
- [ ] commit message: `release: vX.Y.Z — <what changed>`
- [ ] tag ที่ root: `git tag vX.Y.Z`
- [ ] push: `git push origin main --tags`
- [ ] verify จาก consumer ที่ใช้ public proxy
