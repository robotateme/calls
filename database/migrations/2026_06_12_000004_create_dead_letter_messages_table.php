<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('dead_letter_messages', function (Blueprint $table): void {
            $table->id();
            $table->string('source', 64)->index();
            $table->string('topic', 128)->index();
            $table->unsignedInteger('message_partition')->nullable();
            $table->unsignedBigInteger('message_offset')->nullable();
            $table->string('message_key')->nullable()->index();
            $table->string('trace_id')->nullable()->index();
            $table->string('reason', 128)->index();
            $table->text('raw_payload');
            $table->json('decoded_payload')->nullable();
            $table->string('message_hash', 64)->unique();
            $table->timestamp('resolved_at')->nullable()->index();
            $table->text('resolution_note')->nullable();
            $table->timestamp('created_at')->nullable();

            $table->index(
                ['source', 'topic', 'message_partition', 'message_offset'],
                'dead_letter_messages_source_position_idx',
            );
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('dead_letter_messages');
    }
};
