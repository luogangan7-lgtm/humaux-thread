//! `#[fail_closed(threat = "...")]` 标注宏（§53.6：缺 threat 参数编译不过；
//! direction table 生成源之一）。Wave3 实现校验逻辑，当前为可编译占位。

use proc_macro::TokenStream;

/// Marks a function as fail-closed for a named threat (§53.6). Wave3 adds the
/// compile-error on missing `threat`.
#[proc_macro_attribute]
pub fn fail_closed(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
