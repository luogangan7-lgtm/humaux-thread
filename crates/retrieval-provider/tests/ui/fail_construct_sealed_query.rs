use humaux_local_secret_scan::SealedRetrievalQuery;

fn main() {
    let _ = SealedRetrievalQuery {
        text: "forged".to_owned(),
        data_class: humaux_domain::dataclass::DataClass::Private,
        receipt: unreachable!(),
    };
}
