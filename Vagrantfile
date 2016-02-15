# -*- mode: ruby -*-
# vi: set ft=ruby :

ip = `ifconfig | grep "inet " | grep -v 127.0.0.1 | cut -f 2 -d " "`

Vagrant.configure(2) do |config|
  config.vm.box = "nrel/CentOS-6.7-x86_64"
  config.vm.box_url = "https://developer.nrel.gov/downloads/vagrant-boxes/CentOS-6.7-x86_64-v20151108.box"
  config.vm.hostname = "cloudbreak"


  config.vm.network "forwarded_port", guest: 3000, host: 3000, auto_correct: true
  
  # Install Cloudbreak when the Vagrant is started
  config.vm.provision "shell" do |s|
    s.path = "install_cloudbreak.sh"
    # s.args = "#{ PUBLIC_IP[ip]}"
  end
  
  config.vm.provider "virtualbox" do |v|
    v.memory = 2048
    v.cpus = 2
  end
  
  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine. In the example below,
  # accessing "localhost:8080" will access port 80 on the guest machine.
  # config.vm.network "forwarded_port", guest: 80, host: 8080

end
