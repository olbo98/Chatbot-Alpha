import argparse
from data_loader import *
from util import *
from stream import Stream

"""
- The data set contains only comments (not the posts that they are commenting on).
- Comments have globally unique IDs (they are not relative to their parent posts).
- Comments are cronologically sorted in the original data set.*
- The comment IDs are also sorted (presumably they are assigned ordered by time).
  (In other words, sorting by created_utc or by id would yield the same ordering.)
  This is good, because then we know comments always appear in the correct order.
  (Replies always after the thing they are replying to.)

* apparently not strictly true in more recent data, but close enough.
"""

class RedditStatsAccumulator:
    """A transparent filter."""

    def __init__(self):
        self.subreddit_posts = {}   # id -> #comments
        self.subreddit_names = {}   # id -> name
        self.posts = 0
        self.pairs = 0
        self.comments = 0
        self.deleted = 0

    def __call__(self, comment):
        self.comments += 1
        pid = get_parent_comment_id(comment)
        if comment['body'] == '[deleted]':
            self.deleted += 1
        if pid.startswith('c'):
            self.pairs += 1
        else:
            self.posts += 1
        subreddit_id = comment['subreddit_id']
        subreddit = comment['subreddit']
        if subreddit_id not in self.subreddit_posts:
            self.subreddit_posts[subreddit_id] = 1
            self.subreddit_names[subreddit_id] = subreddit
        else:
            self.subreddit_posts[subreddit_id] += 1
            if self.subreddit_names[subreddit_id] != subreddit:
                print(' ! inconsistent mapping')
        return comment

    # TODO: deprecated
    def get_stats(self):
        return {'subreddit_posts': self.subreddit_posts,
                'subreddit_names': self.subreddit_names,
                'posts': self.posts,
                'pairs': self.pairs,
                'comments': self.comments,
                'deleted': self.deleted,
                }

    def show(self):
        subreddits = self.subreddit_posts
        subreddit_ids = sorted(subreddits.keys(), key=lambda k: subreddits[k])
        print('='*80)
        print('Metadata summary')
        print('='*80)
        print('Subreddit (id): #posts')
        print('-'*40)
        for subreddit in subreddit_ids:
            print(self.subreddit_names[subreddit] + ' (' + subreddit + '): ' + str(subreddits[subreddit]))
        print('-'*40)
        print('Subreddits: ' + str(len(subreddit_ids)))
        print('Deleted comments: ' + str(self.deleted))
        print('On original posts: ' + str(self.posts))
        print('Comment-reply pairs: ' + str(self.pairs))
        print('Total comments: ' + str(self.comments))

def in_subreddit(*subreddits):
    return lambda comment: comment['subreddit'] in subreddits

def not_deleted(comment):
    return comment['body'] != '[deleted]'

def on_comment(comment):
    """
    Note that filtering on this makes sense in some situations but not
    in others. We don't want to ignore comments on original posts, even
    though we don't have that post, because there could be comments *on
    this comment*. But if we want to look at the comment/parent IDs and
    see which pairs exist, then we can ignore comments that are not
    replies to other comments, because we are never going to find the
    parent.
    For example:
    - ??? (id=1)
        - id=2, parent=1
            - id=4, parent=2
        - id=3, parent=1
    Here comment 1 is some original post that we don't have. So the
    pairs (2,1) and (3,1) are worthless, but pair (4,2) is not!
    """
    return comment['parent_id'].find('_c') >= 0

def group_by_parent_post(comments):
    """
    Return a dictionary that maps parent_ids to comments.
    """
    d = {}
    for comment in comments:
        parent = comment['parent_id']
        if parent in d:
            d[parent].append(comment)
        else:
            d[parent] = [comment]
    return d

def get_parent_comment_id(comment):
    pid = comment['parent_id']
    index = pid.index('_')  # Error if '_' is missing; *should* be fine.
    return pid[index+1:]

def count_reply_pairs(*files):
    return get_comment_pairs(read_comments(*files).filter(not_deleted)).count()

def get_subreddits(*files):
    one = lambda x: 1
    add = lambda a,b: a + b
    return (read_comments(*files)
            .to_dict(extract_key('subreddit'), one, add))

def get_comment_pairs_ids(stream):
    """
    It seems like (unless there are some exceptions somewhere) the comment
    IDs are consistently named such that a reply to a comment with ID 'c...'
    will have a `parent_id` of 'tx_c...', where x is some digit.
    So this function simply extracts the `id` and `parent_id` from a comment.
    Note that the `parent_id` is transformed to be of the form `c...` so that
    it can actually be used later, instead of some garbage that cannot be
    looked up. That way, reconstructing the full comment-reply pairs can just
    involve looking up the actual comment IDs in a dictionary, for example.
    """
    # Only count comments that are replies on other comments.
    def f():
        for comment in it.filter(on_comment):
            pid = get_parent_comment_id(comment)
            yield (comment.id, pid)
    return Stream(f())

def get_comment_pairs(stream):
    """
    Like get_comment_pairs_ids but returns pairs of actual comments (dicts)
    rather than IDs. (Needs to buffer all comments it has ever seen.)
    """
    def f():
        parents = {}
        for comment in stream.filter(on_comment):
            parents[comment['id']] = comment
            pid = get_parent_comment_id(comment)
            if pid in parents:
                yield (comment, parents[pid])
    return Stream(f())

def get_replies(comments, comment):
    cid = comment['id']
    replies = []
    for other in comments:
        parent = other['parent_id']
        if parent.endswith(cid):
            replies.append(other)
    return replies

def show_comment_recursively(comments, comment, level=0, show_header=True, clean=True):
    """
    Recreates the comment threads. Comments are assumed to be raw.
    (They could be preprocessed, but then they must at least still have their
    'author' fields.)
    Only shows comment ID, author, and comment body.
    When clean is true, do some minimal cleanup (remove excess whitespace).
    """
    regex = re.compile(r'\s+')
    indentation = '    '*level
    body = comment['body']
    cid = comment['id']
    author = comment['author'] if show_header else ''
    comments.remove(comment)    # Done. We don't want to print this agin.
    header = indentation + '- '
    if show_header:
        header += cid + ', ' + author + ':\n  ' + indentation
    if clean:
        body = re.sub(regex, ' ', body.strip())
    # Print this message.
    print(header + str(body))
    # Then recursively show replies.
    for reply in get_replies(comments, comment):
        show_comment_recursively(comments, reply, level+1, show_header, clean)

# Note: Consumes the stream!
def show_conversations(comments, limit=20, show_header=True, clean=True):
    """Recreates the comment threads."""
    comments = list(comments)
    count = 0
    # Can't iterate in a cleaner way because show_comment modifies the list.
    while (limit <= 0 or count <= limit) and len(comments) > 0:
        show_comment_recursively(comments, comments[0], show_header=show_header, clean=clean)
        count += 1

def _preprocessor_pipeline(args):
    funcs = []
    key = args.text_field
    if args.strip_specials:
        funcs.append(wrap(keep_alnum(), key))
    if args.strip_digits:
        funcs.append(wrap(strip_digits(), key))
    if args.trim_whitespace:
        funcs.append(wrap(trim_whitespace(), key))
    if args.to_lower:
        funcs.append(wrap(str.lower, key))
    if args.tokenize:
        funcs.append(wrap(str.split, key))
    return preprocessor_pipeline(funcs)

def _subreddit_filter(args):
    if args.keep_subreddits is not None:
        subreddits = set(args.keep_subreddits)
        return lambda comment: comment['subreddit'] in subreddits
    elif args.strip_subreddits is not None:
        subreddits = set(args.strip_subreddits)
        return lambda comment: comment['subreddit'] not in subreddits
    else:
        return lambda comment: True

def _field_filter(args):
    if args.keep_fields is not None:
        fields = set(args.keep_fields)
        return lambda record: keep_fields(record, fields)
    elif args.strip_fields is not None:
        fields = set(args.strip_fields)
        return lambda record: strip_fields(record, fields)
    else:
        return nop

def _show_pair(comment, keep_parent=False):
    if on_comment(comment):
        pid = comment['parent_id'] if keep_parent else get_parent_comment_id(comment)
        print((comment['id'], pid))

def _show_pairs(keep_parent=False):
    def f(comment):
        _show_pair(comment, keep_parent)
    return f

def _show_conversations(stream, show_header):
    show_conversations(stream, limit=-1, show_header=args.show_header, clean=False)

def _main(args):
    list_fields = args.list_fields or args.count_fields or args.count_field_values
    # Set up the stream ...
    stream = read_records(*args.file)
    if args.read_max is not None:
        stream = stream.take(args.read_max)
    if args.ignore_deleted:
        stream = stream.filter(not_deleted)
    if args.process_max is not None:
        stream = stream.take(args.process_max)
    stream = stream.filter(_subreddit_filter(args))
    stream = stream.filter(_field_filter(args))
    reddit_stats = nop
    if args.summary:
        reddit_stats = RedditStatsAccumulator()
        stream = stream.map(reddit_stats)
    stats = StatsAccumulator(track_values=args.count_field_values)
    stream = stream.map(stats) if list_fields else stream
    stream = stream.map(_preprocessor_pipeline(args))
    if args.max_length:
        stream = stream.filter(max_text_length(args.max_length))
    if args.min_length:
        stream = stream.filter(min_text_length(args.min_length))
    encoder = Encoder()
    stream = stream.map(wrap(encoder)) if args.vocab else stream
    if args.show_records:
        stream = stream.peek(println)
    if args.pairs:
        stream = stream.peek(_show_pairs(args.keep_parent))
    if args.conversations:
        _show_conversations(stream, show_header=args.show_header)
    else:
        _ = stream.count()  # consume stream
    # Show the results ...
    if args.vocab:
        show_vocab(encoder, sort_by=args.vocab_order, reverse=args.reverse, brief=args.brief)
    if args.summary:
        #_show_summary(reddit_stats)
        reddit_stats.show()
    if list_fields:
        stats.show(args.count_fields)

if __name__ == '__main__':
    description = """
    Parse and transform reddit comments (or other JSON data).  Useful
    both for exploring the data set and for doing actual preprocessing.
    Can read JSON from plain text or BZip2-compressed files.
    All operations EXCEPT FOR `--conversations` can essentially handle
    arbitrarily large amounts of data.  (More data will take more time,
    but you won't run out of memory.)

    The following options are only relevant to reddit comments
    specifically, and won't work (or make sense) with other data sets:
    --summary, --(no)-ignore-deleted, --keep/strip-subreddits,
    --pairs, --conversations.
    """
    epilog = """
    Summary, pairs, vocab, list fields, and conversations CAN all
    be combined.
    But `--conversations` needs O(n) memory and O(n²) time.
    So don't use that if you plan to process lots of data.
    The others need (more or less) O(1) memory and O(n) time.
    An exception that could potentially cause problems is listing
    fields, especially with `--count-field-values`, since they need to
    accumulate data, but it shoudln't be a problem in most cases.
    (For example, counting how often different comment bodies appear
    would be a very stupid idea unless the data set is small.)
    Summary is the only one that will not potentially produce millions
    of lines of output.

    Note that, when reading BZip2-compressed files, the processing
    time will almost certainly be completely dominated by the
    decompression.
    """
    parser = argparse.ArgumentParser(description=description, epilog=epilog)
    parser.add_argument('file', nargs='+', help='the files you want to process (plain or .bz2). Separate from previous args with -- if necessary.')
    mutex = parser.add_mutually_exclusive_group()
    mutex.add_argument('--ignore-deleted', action='store_true', default=False, help='completely remove deleted comments from consideration (reddit only)')
    mutex.add_argument('--no-ignore-deleted', action='store_false', dest='ignore_deleted', help='do not ignore deleted comments (this is the default) (reddit only)')

    # TODO: add --sort-fields
    parser.add_argument('--text-field', default='body', help='the field containing the actual text')
    parser.add_argument('--show-records', action='store_true', help='print the records')
    parser.add_argument('--read-max', type=int, help='read at most this many records from files')
    parser.add_argument('--process-max', type=int, help='process at most this many records (same as --read-max when not ignoring deleted)')
    # Not offering a --print-max. That's what less is for.

    fields = parser.add_argument_group(title='List fields', description='List the different fields')
    fields.add_argument('--list-fields', action='store_true', help='show the set of fields')
    fields.add_argument('--count-fields', action='store_true', help='show how often the fields appear in records')
    fields.add_argument('--count-field-values', action='store_true', help='show the values that fields take on (may potentially use lots of memory')

    fields.add_argument('--distinct-keys', action='store_true', help="don't repeat fields")
    fields.add_argument('--distinct-values', action='store_true', help="don't repeat field values")

    stats = parser.add_argument_group(title='Summary', description='Brief metadata summary.')
    stats.add_argument('--summary', action='store_true', help='show brief metadata summary (reddit only)')

    vocab = parser.add_argument_group(title='Vocabulary', description='Show the vocabulary of the corpus.')
    vocab.add_argument('--vocab', action='store_true', help='show the vocabulary')
    mutex = vocab.add_mutually_exclusive_group()
    mutex.add_argument('--by-count', action='store_const', dest='vocab_order', const='count', help='sort by occurrences')
    mutex.add_argument('--by-index', action='store_const', dest='vocab_order', const='index', help='sort by index')
    mutex.add_argument('--by-word', action='store_const', dest='vocab_order', const='word', help='sort alphabetically by word')
    mutex.add_argument('--brief', action='store_true', help="only summarize; don't list the full vocabulary")
    vocab.add_argument('--reverse', action='store_true', help='reversed sorting order')

    pairs = parser.add_argument_group(title='Pairs', description='Show all comment-reply pairs.')
    pairs.add_argument('--pairs', action='store_true', help='list all comment pairs (long). Could be redirected to a file as a separate preprocessing step. (reddit only)')
    pairs.add_argument('--keep-parent', action='store_true', help='by default, the parent ID is transformed to match the comment ID of the parent. Set this to keep the original parent ID.')

    conv = parser.add_argument_group(title='Conversations', description='Print the discussion thread reconstruction')
    conv.add_argument('--conversations', action='store_true', help='show conversations (cannot handle large data sets). (reddit only)')
    conv.add_argument('--no-header', action='store_false', dest='show_header', help="don't show a header at the top of comments")

    preproc = parser.add_argument_group(title='Preprocessing', description='Do preprocessing on body text and/or remove xor keep certain fields.')
    strip_or_keep = preproc.add_mutually_exclusive_group()

    # TODO: make some of these default and add a --raw?
    preproc.add_argument('--trim-whitespace', action='store_true', help='remove excess whitespace from comment bodies, both at the start and end, but also within the text')
    preproc.add_argument('--strip-specials', action='store_true', help='remove special characters from comment bodies')
    preproc.add_argument('--tokenize', action='store_true', help='split into words')
    preproc.add_argument('--strip-digits', action='store_true', help='remove digits from comment bodies')
    preproc.add_argument('--to-lower', action='store_true', help='transform comment bodies to lower case')
    preproc.add_argument('--min-length', type=int, help='minimum length')
    preproc.add_argument('--max-length', type=int, help='maximum length')

    strip_or_keep.add_argument('--strip-fields', nargs='+', help='strip these fields')
    strip_or_keep.add_argument('--keep-fields', nargs='+', help='keep these fields')

    mutex = preproc.add_mutually_exclusive_group()
    mutex.add_argument('--strip-subreddits', nargs='+', help='remove comments from subreddit(s) (reddit only)')
    mutex.add_argument('--keep-subreddits', nargs='+', help='keep comments from subreddit(s) (reddit only)')

    args = parser.parse_args()
    _main(args)
