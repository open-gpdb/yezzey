#pragma once

#include <utility>

template <typename F> class ScopeGuard {
public:
  explicit ScopeGuard(F fn) : fn_(std::move(fn)), active_(true) {}
  ScopeGuard(ScopeGuard &&other) noexcept
      : fn_(std::move(other.fn_)), active_(other.active_) {
    other.active_ = false;
  }
  ~ScopeGuard() {
    if (active_) {
      fn_();
    }
  }
  ScopeGuard(const ScopeGuard &) = delete;
  ScopeGuard &operator=(const ScopeGuard &) = delete;
  ScopeGuard &operator=(ScopeGuard &&) = delete;

  /* cancel the pending action (e.g. on the success path) */
  void dismiss() { active_ = false; }

private:
  F fn_;
  bool active_;
};

template <typename F> ScopeGuard<F> makeScopeGuard(F fn) {
  return ScopeGuard<F>(std::move(fn));
}
