resource "local_file" "pet" {
    filename = "pets.txt"
    content = "We love INDIA!!!!"
    file_permission = "0777"

    lifecycle {
        # create_before_destroy = true
        # prevent_destroy = true
        ignore_changes = true
    }
}