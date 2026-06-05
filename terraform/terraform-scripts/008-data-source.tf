resource "local_file" "pet" {
  filename = "pets.txt"
  content  = data.local_file.pet.content
}

data "local_file" "pet" {
  filename = "dog.txt"
}