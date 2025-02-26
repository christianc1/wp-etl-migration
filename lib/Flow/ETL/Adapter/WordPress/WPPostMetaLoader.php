<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use Flow\ETL\Adapter\WordPress\RowsNormalizer\EntryNormalizer;
use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

final class WPPostMetaLoader implements Loader
{
    private string $dateTimeFormat = \DateTimeInterface::ATOM;

    public function __construct(
        private readonly array $config = []
    ) {
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $normalizer = new RowsNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            $this->insertPostMeta($normalizedRow);
        }
    }

    private function insertPostMeta(array $data): bool
    {
        if (empty($data['post.ID'])) {
            throw new RuntimeException('Post ID is required');
        }

        $postId = (int) $data['post.ID'];

        // Verify post exists
        if (!get_post($postId)) {
            throw new RuntimeException("Post with ID {$postId} does not exist");
        }

        // Process all meta.* entries
        $metaUpdated = false;
        foreach ($data as $key => $value) {
            if (str_starts_with($key, 'meta.')) {
                $metaKey = substr($key, 5); // Remove 'meta.' prefix

                $result = update_post_meta( absint( $postId ), sanitize_key( $metaKey ), $value );

                if (false === $result && !is_null($result)) {
                    throw new RuntimeException("Failed to update post meta for post ID {$postId} with key {$metaKey}");
                }

                $metaUpdated = true;
            }
        }

        if (!$metaUpdated) {
            throw new RuntimeException('No meta fields found to update');
        }

        return true;
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }
}
