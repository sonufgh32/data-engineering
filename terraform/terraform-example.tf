resource "local_file" "pet" {
    filename = "pets.txt"
    content = "We love INDIA!!!!"
    file_permission = "0700"
}

resource "local_sensitive_file" "games" {
  filename     = "protected_pets"
  content = "FIFA 21"
}
