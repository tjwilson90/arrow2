(function() {var implementors = {};
implementors["arrow2"] = [{"text":"impl&lt;'a&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"arrow2/bitmap/struct.BitmapIter.html\" title=\"struct arrow2::bitmap::BitmapIter\">BitmapIter</a>&lt;'a&gt;","synthetic":false,"types":["arrow2::bitmap::iterator::BitmapIter"]},{"text":"impl&lt;T:&nbsp;<a class=\"trait\" href=\"arrow2/types/trait.BitChunk.html\" title=\"trait arrow2::types::BitChunk\">BitChunk</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"arrow2/types/struct.BitChunkIter.html\" title=\"struct arrow2::types::BitChunkIter\">BitChunkIter</a>&lt;T&gt;","synthetic":false,"types":["arrow2::types::bit_chunk::BitChunkIter"]},{"text":"impl&lt;R:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/std/io/trait.Read.html\" title=\"trait std::io::Read\">Read</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/std/io/trait.Seek.html\" title=\"trait std::io::Seek\">Seek</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"arrow2/io/ipc/read/struct.FileReader.html\" title=\"struct arrow2::io::ipc::read::FileReader\">FileReader</a>&lt;R&gt;","synthetic":false,"types":["arrow2::io::ipc::read::reader::FileReader"]},{"text":"impl&lt;R:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/std/io/trait.Read.html\" title=\"trait std::io::Read\">Read</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"arrow2/io/ipc/read/struct.StreamReader.html\" title=\"struct arrow2::io::ipc::read::StreamReader\">StreamReader</a>&lt;R&gt;","synthetic":false,"types":["arrow2::io::ipc::read::stream::StreamReader"]},{"text":"impl&lt;T:&nbsp;<a class=\"trait\" href=\"arrow2/types/trait.BitChunk.html\" title=\"trait arrow2::types::BitChunk\">BitChunk</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"arrow2/bits/struct.BitChunks.html\" title=\"struct arrow2::bits::BitChunks\">BitChunks</a>&lt;'_, T&gt;","synthetic":false,"types":["arrow2::bits::chunk_iterator::BitChunks"]},{"text":"impl&lt;'a&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"arrow2/bits/struct.SlicesIterator.html\" title=\"struct arrow2::bits::SlicesIterator\">SlicesIterator</a>&lt;'a&gt;","synthetic":false,"types":["arrow2::bits::slice_iterator::SlicesIterator"]},{"text":"impl&lt;'a, T, I:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a>&lt;Item = T&gt;&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"arrow2/bits/struct.ZipValidity.html\" title=\"struct arrow2::bits::ZipValidity\">ZipValidity</a>&lt;'a, T, I&gt;","synthetic":false,"types":["arrow2::bits::zip_validity::ZipValidity"]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()