fn main() {
    let src = "third_party/tetra-codec/source";
    println!("cargo:rerun-if-changed={src}/tetra-codec.c");
    println!("cargo:rerun-if-changed={src}/tetra-codec-impl.c");
    println!("cargo:rerun-if-changed={src}/tetra-codec-impl.h");
    println!("cargo:rerun-if-changed=third_party/tetra-codec/include/tetra-codec.h");

    cc::Build::new()
        .file(format!("{src}/tetra-codec.c"))
        .file(format!("{src}/tetra-codec-impl.c"))
        .include(src)
        .include("third_party/tetra-codec/include")
        .warnings(false)
        .compile("tetra-codec");
}
