# typed: false
# frozen_string_literal: true

class KafscaleCli < Formula
  desc "Command line tool for KafScale"
  homepage "https://kafscale.io"
  version "2.1.0"
  license "Apache-2.0"

  on_macos do
    if Hardware::CPU.intel?
      url "https://github.com/KafScale/platform/releases/download/v2.1.0/kafscale-cli_v2.1.0_darwin_amd64.tar.gz"
      sha256 "82bc172a66c3bad554ec48f68ec3f498d4a1c8727f124f07bae4f77803aa8d9a"

      define_method(:install) do
        bin.install "kafscale-cli"
      end
    end
    if Hardware::CPU.arm?
      url "https://github.com/KafScale/platform/releases/download/v2.1.0/kafscale-cli_v2.1.0_darwin_arm64.tar.gz"
      sha256 "8cf4ce4cc0d608591e96c81d9d82aa15a6313824bfd2bcaaf62a5d01c032d073"

      define_method(:install) do
        bin.install "kafscale-cli"
      end
    end
  end

  on_linux do
    if Hardware::CPU.intel? && Hardware::CPU.is_64_bit?
      url "https://github.com/KafScale/platform/releases/download/v2.1.0/kafscale-cli_v2.1.0_linux_amd64.tar.gz"
      sha256 "0f933fd7d7c2417156c9c20175448ed64dcb12b199a1c646225dcea4fc557422"

      define_method(:install) do
        bin.install "kafscale-cli"
      end
    end
    if Hardware::CPU.arm? && Hardware::CPU.is_64_bit?
      url "https://github.com/KafScale/platform/releases/download/v2.1.0/kafscale-cli_v2.1.0_linux_arm64.tar.gz"
      sha256 "aebdd36ca59a3589da31afa8f2114a200bbf3fc604a5a6f6b0d7df672d6e58cf"

      define_method(:install) do
        bin.install "kafscale-cli"
      end
    end
  end

  test do
    system "#{bin}/kafscale-cli", "--version"
  end
end
