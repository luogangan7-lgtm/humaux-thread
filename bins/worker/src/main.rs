//! `worker::main` — `humaux-worker` 进程入口（最小必要进程集见 §4.2；admin 探针契约见 §4.4）。
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: [process(humaux-worker)]
//! Invariants: []
//! Spec: Baseline §4; §4.2; §4.4; §67.2

fn main() {
    // T 后续任务接线；进程职责与凭证边界以 §4 / §67.2 为准。
    println!("humaux-worker: not wired yet (Phase 0 scaffold)");
}
