class AgentOs < Formula
  desc "Local control plane for AI agents, tasks, memory, and execution events"
  homepage "https://github.com/Shree-git/agent-os"
  license "MIT"
  version "0.1.0"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/Shree-git/agent-os/releases/download/v0.1.0/agent-os-0.1.0-aarch64-apple-darwin.tar.gz"
      sha256 "REPLACE_WITH_AARCH64_APPLE_DARWIN_SHA256"
    else
      url "https://github.com/Shree-git/agent-os/releases/download/v0.1.0/agent-os-0.1.0-x86_64-apple-darwin.tar.gz"
      sha256 "REPLACE_WITH_X86_64_APPLE_DARWIN_SHA256"
    end
  end

  on_linux do
    if Hardware::CPU.intel?
      url "https://github.com/Shree-git/agent-os/releases/download/v0.1.0/agent-os-0.1.0-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "REPLACE_WITH_X86_64_UNKNOWN_LINUX_GNU_SHA256"
    else
      odie "Agent OS Homebrew formula currently supports x86_64 Linux, x86_64 macOS, and Apple Silicon macOS"
    end
  end

  def install
    bin.install "bin/agent-os"
    bash_completion.install "completions/agent-os.bash" => "agent-os"
    zsh_completion.install "completions/agent-os.zsh" => "_agent-os"
    fish_completion.install "completions/agent-os.fish"
    (pkgshare/"completions").install "completions/agent-os.elvish", "completions/agent-os.powershell"
  end

  test do
    assert_match "agent-os", shell_output("#{bin}/agent-os --help")
    system "#{bin}/agent-os", "completions", "bash"
    assert_path_exists pkgshare/"completions/agent-os.elvish"
    assert_path_exists pkgshare/"completions/agent-os.powershell"
  end
end
