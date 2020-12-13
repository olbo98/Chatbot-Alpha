# Chatbot-Alpha
Chatbot for LTU course D7046E

## `reddit_loader`

### Handling large amounts of data

The complete (compressed) data set is about 300 GB. Uncompressed, that is
roughly 1.8 TB. That is both too much to process and also (probably) too much
to train on.

Yet, we may want to use at least a few GB of data. It would also be nice to be
**able** to use as much data as possible, even if we choose not to.  So the
data loading code was written with a focus on being able to handle large
volumes.

The amount of data will shrink as it is being processed:

- Removing unnecessary fields.
- Removing inappropriate and/or redundant characters.
- Removing redundancy in the form of differences in case ("Hello" == "hello").
- Removing comments with sentences that are too long or too short.
- Removing comments that are not paired with a reply.
- Encoding (removing redundancy in the sense that each word is only stored
  once and represented by an integer).
- Possibly removing words that appear very infrequently.

This means that an amount of data that we can handle during training was
originally a much bigger amount of data. So if we want to be able to use as
much data as possible for training, we don't want to be limited by the amount
we are able to read in.

An obvious solution to this is to avoid first reading in all the data and then
start processing it, and instead process it while it is being read. In other
words, it would be great if we could set up a stream of data that can be
consumed as it is being produced, and no large amounts of data need to be
accumulated.

This is easily solved. The `Stream` class only makes some things more
convenient from a usability standpoint. However, there is a fundamental
limitation.

#### Getting comment-reply pairs

We are interested in extracting pairs of comments, where one comment is a
direct reply to another comment. Finding the pairs (in terms of IDs) is
easy, since each comment carries with it the ID of the comment it is replying
to. However, at some point we have to get hold of the actual comment rather
than just its ID, so that we can access the text. The straightforward solution
to that is to accumulate a dictionary where IDs map to comments. Then, when
a comment is processed, we can use its parent ID to retreive the matching
comment from the dictionary. Unfortunately, that means all comments have to
be accumulated after all, because for each comment, we only know whether it
will be paired with another comment after having examined all of the remaining
comments.

(For some comments, we can tell immediately whether they are paired, but not
always. For example, the data set includes only comments, not original posts.
So for comments on posts rather than other comments, we cannot know whether
they appear in a pair, because we don't yet know whether a reply will appear.
We only know that it does not form a pair with whatever it is itself replying
to. Some comments may also reply to comments that have been deleted, which
creates the same situation. It does not form a pair with the comment it is
replying to, but another comment we haven't seen yet could reply to it.)

One possible way to mitigate that is to load the data in two steps.

First, find all the pairs. This can be done with constant memory. So in terms
of memory, this operation is free. Now we have the IDs of all the comments that
appear in pairs. So then, step two, we can read the data again and immediately
discard all comments that are not in a pair.

Of course, in terms of time, it's certainly not free. Reading BZip2-compressed
files is very CPU intensive, and just the decompression (with negligible
processing) can easily take on the order of half an hour for a few GB.

Either BZip2 decompression cannot be done in parallel, or the common
implementations simply don't do it. Either way, multiple cores don't help.
Or at least they don't help with reading a single file. But what if we could
read (and decompress) multiple files at once, each in a separate thread, and
then combine the input streams into one, so we get a single stream of input,
we would essentially be parallelizing the decompression. It's just parallelism
at the level of the data set rather than individual files.

While Python does have support for threads, doing this in Java is extremely
easy. So I propose to use a small Java program to generate an auxiliary
data set that can be reused later.

Although, the second phase could also benefit from parallelism.


### Basic features

Some basic features of `reddit_loader`:

- Can read plain and BZip2-compressed files.
- Can read multiple files and return a single stream.
- Contains some simple tools:
  - encoder
  - preprocessing functions (for filtering records and transforming text)

### `reddit_loader` as a standalone program

While it is not intended to be executed directly as an application during
training, it can be used to explore the data, or possibly for doing some
preprocessing where intermediate results can be redirected to a file and
used later.

It has X main functions:

- Showing a summary of the data format.
- Showing the different fields.
- Showing comment pairs.
- Reconstructing the discussion threads.
- Showing the vocabulary.
- Doing filtering and showing the resulting records.

 (and optionally how often they take on certain values)

#### Summary

Running `$ python3 reddit_loader.py --summary RC_2006*` gives the output:

```
================================================================================
Metadata summary
================================================================================
Subreddit (id): #posts
----------------------------------------
es (t5_22i2): 2
zh (t5_22i5): 2
vi (t5_247f): 2
fr (t5_22i1): 3
hu (t5_2476): 4
olympics (t5_21of): 4
eo (t5_247a): 4
nl (t5_247d): 6
programming (t5_2fwo): 6
ru (t5_247i): 14
de (t5_22i0): 27
no (t5_247e): 38
tr (t5_2478): 40
nsfw (t5_vf2): 60
ja (t5_22i6): 102
request (t5_21nj): 157
features (t5_21n6): 204
reddit.com (t5_6): 12086
----------------------------------------
Subreddits: 18
Deleted comments: 1808
On original posts: 7012
Comment-reply pairs: 5749
Total comments: 12761
```


#### List fields

Running `$ python3 reddit_loader.py --list-fields RC_2006*` gives the output:

```
author
ups
parent_id
id
link_id
author_flair_text
score
distinguished
stickied
author_flair_css_class
gilded
created_utc
controversiality
subreddit
edited
subreddit_id
body
retrieved_on
```

Running `$ python3 reddit_loader.py --list-fields --count-fields RC_2006*` is
not very interesting in this case, but it lets us confirm that all records have
the same fields (for 2006-01 and 2006-02, they all have the count 12761).

Running `$ python3 reddit_loader.py --list-fields --count-field-values
RC_2006*` would produce a lot of output, but we could add `--keep-fields
stickied edited`, for example, and we can still have `--count-fields` if
we want to. Then we get:

```
stickied: 12761
  False: 12761
edited: 12761
  False: 12745
  True: 1
  1473217347.0: 1
  1473218577.0: 1
  1473217353.0: 1
  1473217239.0: 1
  1472971788.0: 1
  1473217359.0: 1
  1436374059.0: 1
  1436388131.0: 1
  1473219573.0: 1
  1473217143.0: 1
  1436390266.0: 1
  1440972539.0: 1
  1436394208.0: 1
  1436389054.0: 1
  1473219423.0: 1
```

Of course, we could recreate some of the summary data with
`$ python3 reddit_loader.py --list-fields --keep-fields subreddit --count-field-values RC_2006*`
and get:

```
subreddit
  request: 157
  features: 204
  nl: 6
  fr: 3
  vi: 2
  tr: 40
  ja: 102
  es: 2
  olympics: 4
  hu: 4
  zh: 2
  nsfw: 60
  eo: 4
  programming: 6
  de: 27
  reddit.com: 12086
  no: 38
  ru: 14
```

Instead of specifying which fields to keep, we can also exclude some with `--strip-fields ...`.


#### Pairs

Running with `--pairs` simply prints all the pairs with their IDs. Adding
`--keep-parent` preserves the original parent ID.  (By default, it is modified
to be the comment ID of the parent, which isn't the same thing.)

There's nothing else that can be done with `--pair` that can't also be done
with the other operations.


#### Vocab

Running with `--vocab` shows which tokens map to which indices and how often
they occur. The sort order can be controlled with `--by-index`, `--by-count`,
`--by-word`, and `--reverse`. But `--vocab` only makes sense with suitable
preprocessing steps, like `--tokenize` and `--to-lower`, for example.


#### Conversations

Running with `--conversations` recreates the discussion threads, showing the
relationship between comments with indentation. `--no-header` can be used to
turn off the header showing comment ID and author (though it should probably be
changed to also include the subreddit). Speaking of subreddits, we could limit
the conversations to specific subreddits with, for example, `--keep-subreddits
programming`, or `--strip-subreddits nsfw`.

The ability to specify which subreddits to consider can of course be useful
when used with `--vocab` as well, to see if/how subreddits differ.


#### Preprocessing

There are some options for filtering text, like `--trim-whitespace`, but there
is also `--min-length` and `--max-length`. The number of records that will be
read can be limited with `--read-max`.

#### Combining options

Most combinations of options work. Some don't make sense together, in
particular, excluding various fields can easily cause problems.

Here's a more complex example. We'll ignore deleted comments, and show conversations in the subreddit features if they have exactly 3 words.


```
$ python3 reddit_loader.py --conversations --keep-subreddits features --no-header --tokenize --min-length 3 --max-length 3 RC_2006*
- ['Ah', 'good', 'point.']
- ['Lamb-Sutton', 'go-kart', 'track']
```

#### Complexity

`n` is the number of records, `f` is the number of fields per record, `v` is
the size of the vocabulary.  Note that `f` is constant and also `f ≪  n` in
general, and while `v` is not necessarily much smaller than `n`, it's
essentially a constant, since it asymptotes as `n` increases.

- Summary: `Θ(n)` time; `Θ(1)` memory.
- List fields: `Θ(n)` time; `Θ(f)` memory, or `O(n)` memory if using `--count-field-values`.
- Pairs: `Θ(n)` time; `Θ(1)` memory.
- Vocabulary: `Θ(n)` time; `Θ(v)` memory.
- Conversations: `Θ(n²)` time; `Θ(n)` memory.
