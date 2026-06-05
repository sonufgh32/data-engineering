variable "filename" {
  default = [
    "file1.txt",
    "file2.txt",
    "file3.txt"
  ]
}
resource "local_file" "pet" {
  filename = var.filename[count.index]
  content  = "Fuckkkk Offffffffff"

  count = length(var.filename)
}